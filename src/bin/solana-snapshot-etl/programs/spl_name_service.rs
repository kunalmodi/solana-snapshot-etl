// Parse and ingest solana-name-service records.
// There isn't much documentation outside of the source code, this is the most comprehensive
// I've seen so far:
//   https://github.com/Bonfida/solana-name-service-guide#domain-reverse-lookup
// The tldr is each account should have a 3-piece header (owner, parent, and type), and
// (possibly) type-specific data in the account.
//
// For Twitter records, this is simple, because the reverse Twitter registry includes both the
// owner and Twitter handle. So all we need to do is ingest those records.
//
// For .sol TLDs, it's a little more complicated. The forward registry stores an owner and the
// user defined account data, and the reverse registry stores the actual domain. The only way
// to associate those is via a PDA with does an (offline) hash of the pubkey.
// Since we can't correlate these in realtime, we instead store (Forward Registry, PDA) and
// (Reverse Registry, Domain) as two separate rows, and correlate them in client code.
//
// To make records easier to ingest and use, we try to do all the processing and joins in
// process and write clean records to the database. This of course loses quite a bit of metadata!

use borsh::BorshDeserialize;
use bs58;
use log::warn;
use sha2::{Digest, Sha256};
use solana_sdk::pubkey::Pubkey;
use solana_snapshot_etl::append_vec::StoredAccountMeta;
use spl_name_service::state::{get_seeds_and_key, HASH_PREFIX};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Mutex;

solana_program::declare_id!("namesLPneVptA9Z5rqUDD9tMTWEJwofgaYwp8cawRkX");

const HEADER_LENGTH: usize = 32 * 3; // 3 pubkeys
const TWITTER_TLD: &str = "4YcexoW3r78zz16J2aqmukBLRwGq6rAvWzJpkYAXqebv";
const TWITTER_REVERSE_NAME_CLASS: &str = "FvPH7PrVrLGKPfqaf3xJodFTjZriqrAXXLTVWEorTFBi";
const SOL_TLD: &str = "58PwtjSDuFHuUkYjH9BYnnQKHfwo9reZhC2zMJv9JPkx";
const SOL_REVERSE_NAME_CLASS: &str = "33m47vH6Eav6jr5Ry86XjhRft2jRBLDnDgPSHoquXi2Z";

#[derive(BorshDeserialize)]
struct NameRecordHeader {
  pub parent_name: Pubkey,
  pub owner: Pubkey,
  pub class: Pubkey,
}

#[derive(BorshDeserialize)]
struct TwitterReverseRegistryState {
  #[allow(dead_code)]
  twitter_registry_key: Pubkey,
  twitter_handle: String,
}

#[derive(BorshDeserialize)]
struct SolReverseRegistryState {
  domain: String,
}

pub(crate) struct Record {
  pub pubkey: Pubkey,
  pub owner: Pubkey,
  pub record_type: String,
  pub record: String,
  pub write_version: u64,
}

struct SolReverseMapping {
  domain: String,
  write_version: u64,
}

struct SolMapping {
  pubkey: Pubkey,
  owner: Pubkey,
  pda: Pubkey,
  write_version: u64,
}

pub(crate) struct NameRecordService {
  twitter_records: Mutex<HashMap<Pubkey, Record>>,
  sol_reverse_mapping: Mutex<HashMap<Pubkey, SolReverseMapping>>,
  sol_mapping: Mutex<HashMap<Pubkey, SolMapping>>,
}

impl NameRecordService {
  pub(crate) fn new() -> Self {
    Self {
      twitter_records: Mutex::new(HashMap::new()),
      sol_reverse_mapping: Mutex::new(HashMap::new()),
      sol_mapping: Mutex::new(HashMap::new()),
    }
  }

  pub(crate) fn insert_account(&self, account: &StoredAccountMeta) {
    let mut data = account.data;
    if data.len() <= HEADER_LENGTH {
      return;
    }

    let header = match NameRecordHeader::deserialize(&mut data) {
      Ok(h) => h,
      Err(e) => {
        warn!("Invalid NameRecordHeader {}: {}", account.meta.pubkey, e);
        return;
      }
    };
    if header.parent_name.to_string() == TWITTER_TLD
      && header.class.to_string() == TWITTER_REVERSE_NAME_CLASS
    {
      let twitter_state = match TwitterReverseRegistryState::deserialize(&mut data) {
        Ok(t) => t,
        Err(e) => {
          warn!(
            "Invalid TwitterReverseRegistryState {}: {}",
            account.meta.pubkey, e
          );
          return;
        }
      };
      if let Some(existing_record) = self
        .twitter_records
        .lock()
        .unwrap()
        .get(&account.meta.pubkey)
      {
        if existing_record.write_version > account.meta.write_version {
          return;
        }
      }
      self.twitter_records.lock().unwrap().insert(
        account.meta.pubkey,
        Record {
          pubkey: account.meta.pubkey,
          owner: header.owner,
          record_type: "twitter".to_string(),
          record: sanitize_str(twitter_state.twitter_handle),
          write_version: account.meta.write_version,
        },
      );
    } else if header.parent_name.to_string() == SOL_TLD {
      let pk = bs58::encode(account.meta.pubkey).into_string();
      let mut hasher = Sha256::new();
      hasher.update((HASH_PREFIX.to_owned() + &pk).as_bytes());
      let hashed_name = hasher.finalize().to_vec();
      let (name_account_key, _) = get_seeds_and_key(
        &spl_name_service::ID,
        hashed_name.clone(),
        Some(&Pubkey::from_str(SOL_REVERSE_NAME_CLASS).unwrap()),
        None,
      );
      if let Some(existing_record) = self.sol_mapping.lock().unwrap().get(&account.meta.pubkey) {
        if existing_record.write_version > account.meta.write_version {
          return;
        }
      }
      self.sol_mapping.lock().unwrap().insert(
        account.meta.pubkey,
        SolMapping {
          pubkey: account.meta.pubkey,
          owner: header.owner,
          pda: name_account_key,
          write_version: account.meta.write_version,
        },
      );
    } else if header.class.to_string() == SOL_REVERSE_NAME_CLASS {
      let sol_state = match SolReverseRegistryState::deserialize(&mut data) {
        Ok(s) => s,
        Err(e) => {
          warn!(
            "Invalid SolReverseRegistryState {}: {}",
            account.meta.pubkey, e
          );
          return;
        }
      };
      if let Some(existing_record) = self
        .sol_reverse_mapping
        .lock()
        .unwrap()
        .get(&account.meta.pubkey)
      {
        if existing_record.write_version > account.meta.write_version {
          return;
        }
      }
      self.sol_reverse_mapping.lock().unwrap().insert(
        account.meta.pubkey,
        SolReverseMapping {
          domain: sol_state.domain,
          write_version: account.meta.write_version,
        },
      );
    }
  }

  pub(crate) fn get_records(&self) -> Vec<Record> {
    let mut records = Vec::new();
    let twitter_records = self.twitter_records.lock().unwrap();
    for (_, record) in twitter_records.iter() {
      records.push(Record {
        pubkey: record.pubkey,
        owner: record.owner,
        record_type: record.record_type.clone(),
        record: record.record.clone(),
        write_version: record.write_version,
      });
    }

    let sol_reverse = self.sol_reverse_mapping.lock().unwrap();
    let sol = self.sol_mapping.lock().unwrap();
    let mut unmatched_records: usize = 0;
    for (_, forward) in sol.iter() {
      if let Some(reverse) = sol_reverse.get(&forward.pda) {
        records.push(Record {
          pubkey: forward.pubkey,
          owner: forward.owner,
          record_type: "sol".to_string(),
          record: reverse.domain.clone(),
          write_version: forward.write_version,
        });
      } else {
        unmatched_records += 1;
      }
    }
    if unmatched_records > 0 {
      warn!(
        "Name Service: could not find match {} sol records",
        unmatched_records
      );
    }

    return records;
  }
}

fn sanitize_str(s: String) -> String {
  s.replace('\0', "")
}
