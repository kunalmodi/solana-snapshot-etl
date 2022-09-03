use borsh::BorshDeserialize;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::warn;
use postgres::{Client, NoTls};
use postgres_types::ToSql;
use rand::Rng;
use solana_sdk::program_pack::Pack;
use solana_sdk::pubkey::Pubkey;
use solana_snapshot_etl::append_vec::{AppendVec, StoredAccountMeta};
use solana_snapshot_etl::parallel::{
    par_iter_append_vecs, AppendVecConsumer, AppendVecConsumerFactory, GenericResult,
};
use solana_snapshot_etl::{append_vec_iter, AppendVecIterator};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time;

use crate::programs::mpl_metadata;
use crate::programs::spl_name_service;

pub(crate) type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub(crate) struct PostgresIndexer {
    connection_str: String,
    batch_size: usize,
    name_service: Arc<spl_name_service::NameRecordService>,

    multi_progress: MultiProgress,
    progress: Arc<Progress>,
}

struct Progress {
    accounts_counter: ProgressCounter,
    token_accounts_counter: ProgressCounter,
    metaplex_accounts_counter: ProgressCounter,
}

pub(crate) struct IndexStats {
    pub(crate) accounts_total: u64,
    pub(crate) token_accounts_total: u64,
    pub(crate) token_metadata_total: u64,
    pub(crate) name_service_total: u64,
}

impl AppendVecConsumerFactory for PostgresIndexer {
    type Consumer = Worker;

    fn new_consumer(&mut self) -> GenericResult<Self::Consumer> {
        let db = Client::connect(&self.connection_str, NoTls)?;
        Ok(Worker {
            db: db,
            batch_size: self.batch_size,
            name_service: Arc::clone(&self.name_service),
            progress: Arc::clone(&self.progress),
            queue_account: Vec::with_capacity(self.batch_size),
            queue_token_account: Vec::with_capacity(self.batch_size),
            queue_token_mint: Vec::with_capacity(self.batch_size),
            queue_token_metadata: Vec::with_capacity(self.batch_size),
        })
    }
}

impl PostgresIndexer {
    pub(crate) fn new(connection_str: String, batch_size: usize) -> Result<Self> {
        // Open database.
        let mut db = Client::connect(&connection_str, NoTls)?;
        db.batch_execute(CREATE_TABLES)?;

        // Create progress bars.
        let spinner_style = ProgressStyle::with_template(
            "{prefix:>13.bold.dim} {spinner} rate={per_sec:>13} total={human_pos:>11}",
        )
        .unwrap();
        let multi_progress = MultiProgress::new();
        let accounts_counter = ProgressCounter::new(
            multi_progress.add(
                ProgressBar::new_spinner()
                    .with_style(spinner_style.clone())
                    .with_prefix("accs"),
            ),
        );
        let token_accounts_counter = ProgressCounter::new(
            multi_progress.add(
                ProgressBar::new_spinner()
                    .with_style(spinner_style.clone())
                    .with_prefix("token_accs"),
            ),
        );
        let metaplex_accounts_counter = ProgressCounter::new(
            multi_progress.add(
                ProgressBar::new_spinner()
                    .with_style(spinner_style)
                    .with_prefix("metaplex_accs"),
            ),
        );

        let name_service = Arc::new(spl_name_service::NameRecordService::new());

        Ok(Self {
            connection_str,
            batch_size,
            name_service,
            multi_progress,
            progress: Arc::new(Progress {
                accounts_counter,
                token_accounts_counter,
                metaplex_accounts_counter,
            }),
        })
    }

    pub(crate) fn insert_all_parallel(
        &mut self,
        iterator: AppendVecIterator,
        num_threads: usize,
    ) -> Result<IndexStats> {
        par_iter_append_vecs(iterator, self, num_threads)?;

        let mut worker = self.new_consumer()?;
        let name_service_total = worker.insert_name_service_records()?;

        let stats = IndexStats {
            accounts_total: self.progress.accounts_counter.get(),
            token_accounts_total: self.progress.token_accounts_counter.get(),
            token_metadata_total: self.progress.metaplex_accounts_counter.get(),
            name_service_total,
        };
        let _ = &self.multi_progress;
        Ok(stats)
    }
}

pub struct Worker {
    db: Client,
    batch_size: usize,
    name_service: Arc<spl_name_service::NameRecordService>,
    progress: Arc<Progress>,

    queue_account: Vec<Account>,
    queue_token_account: Vec<TokenAccount>,
    queue_token_mint: Vec<TokenMint>,
    queue_token_metadata: Vec<TokenMetadata>,
}

struct Account {
    pub pubkey: Pubkey,
    pub data_len: u64,
    pub lamports: u64,
    pub owner: Pubkey,
    pub executable: bool,
    pub rent_epoch: u64,
    pub write_version: u64,
}

struct TokenAccount {
    pub pubkey: Pubkey,
    pub mint: Pubkey,
    pub owner: Pubkey,
    pub amount: u64,
    pub delegate: Option<Pubkey>,
    pub state: u8,
    pub is_native: Option<u64>,
    pub delegated_amount: u64,
    pub close_authority: Option<Pubkey>,
    pub write_version: u64,
}

struct TokenMint {
    pub pubkey: Pubkey,
    pub mint_authority: Option<Pubkey>,
    pub supply: u64,
    pub decimals: u8,
    pub is_initialized: bool,
    pub freeze_authority: Option<Pubkey>,
    pub write_version: u64,
}

struct TokenMetadata {
    pub pubkey: Pubkey,
    pub mint: Pubkey,
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub seller_fee_basis_points: u16,
    pub primary_sale_happened: bool,
    pub is_mutable: bool,
    pub edition_nonce: Option<u8>,
    pub collection_verified: Option<bool>,
    pub collection_key: Option<Pubkey>,
    pub write_version: u64,
}

impl AppendVecConsumer for Worker {
    fn on_append_vec(&mut self, append_vec: AppendVec) -> GenericResult<()> {
        for acc in append_vec_iter(Rc::new(append_vec)) {
            self.insert_account(&acc.access().unwrap())?;
        }
        self.finish_append()?;
        Ok(())
    }
}

impl Worker {
    fn insert_account(&mut self, account_meta: &StoredAccountMeta) -> Result<()> {
        let account = Account {
            pubkey: account_meta.meta.pubkey,
            lamports: account_meta.account_meta.lamports,
            owner: account_meta.account_meta.owner,
            executable: account_meta.account_meta.executable,
            rent_epoch: account_meta.account_meta.rent_epoch,
            data_len: account_meta.meta.data_len,
            write_version: account_meta.meta.write_version,
        };

        self.queue_account.push(account);
        if self.queue_account.len() >= self.batch_size {
            self.insert_account_all()?;
        }

        if account_meta.account_meta.owner == spl_token::id() {
            self.insert_token(account_meta)?;
        }
        if account_meta.account_meta.owner == mpl_metadata::id() {
            self.insert_token_metadata(account_meta)?;
        }
        if account_meta.account_meta.owner == spl_name_service::id() {
            self.name_service.insert_account(account_meta);
        }
        self.progress.accounts_counter.inc();
        Ok(())
    }

    fn finish_append(&mut self) -> Result<()> {
        self.insert_account_all()?;
        self.insert_token_account_all()?;
        self.insert_token_mint_all()?;
        self.insert_token_metadata_all()?;
        Ok(())
    }

    fn insert_account_all(&mut self) -> Result<()> {
        if self.queue_account.len() == 0 {
            return Ok(());
        }
        let mut params_prep: Vec<Box<dyn ToSql + Sync>> = Vec::new();
        let mut cnt = 0;
        for account in self.queue_account.drain(..) {
            params_prep.push(Box::new(pk(account.pubkey)));
            params_prep.push(Box::new(u64_sql(account.data_len)));
            params_prep.push(Box::new(pk(account.owner)));
            params_prep.push(Box::new(u64_sql(account.lamports)));
            params_prep.push(Box::new(account.executable));
            params_prep.push(Box::new(u64_sql(account.rent_epoch)));
            params_prep.push(Box::new(u64_sql(account.write_version)));
            cnt += 1;
        }

        let query = INSERT_ACCOUNT.replace("{}", &Self::generate_values(cnt, 7));
        Self::execute_with_retry(&mut self.db, query, params_prep, 1)?;
        Ok(())
    }

    fn insert_token(&mut self, account_meta: &StoredAccountMeta) -> Result<()> {
        match account_meta.meta.data_len as usize {
            spl_token::state::Account::LEN => {
                if let Ok(token_account) = spl_token::state::Account::unpack(account_meta.data) {
                    let ta = TokenAccount {
                        pubkey: account_meta.meta.pubkey,
                        mint: token_account.mint,
                        owner: token_account.owner,
                        amount: token_account.amount,
                        delegate: token_account.delegate.into(),
                        state: token_account.state as u8,
                        is_native: token_account.is_native.into(),
                        delegated_amount: token_account.delegated_amount,
                        close_authority: token_account.close_authority.into(),
                        write_version: account_meta.meta.write_version,
                    };
                    self.queue_token_account.push(ta);
                    if self.queue_token_account.len() >= self.batch_size {
                        self.insert_token_account_all()?;
                    }
                }
            }
            spl_token::state::Mint::LEN => {
                if let Ok(token_mint) = spl_token::state::Mint::unpack(account_meta.data) {
                    let tm = TokenMint {
                        pubkey: account_meta.meta.pubkey,
                        mint_authority: token_mint.mint_authority.into(),
                        supply: token_mint.supply,
                        decimals: token_mint.decimals,
                        is_initialized: token_mint.is_initialized,
                        freeze_authority: token_mint.freeze_authority.into(),
                        write_version: account_meta.meta.write_version,
                    };
                    self.queue_token_mint.push(tm);
                    if self.queue_token_mint.len() >= self.batch_size {
                        self.insert_token_mint_all()?;
                    }
                }
            }
            spl_token::state::Multisig::LEN => {
                return Ok(());
            }
            _ => {
                warn!(
                    "Token program account {} has unexpected size {}",
                    account_meta.meta.pubkey, account_meta.meta.data_len
                );
                return Ok(());
            }
        }
        self.progress.token_accounts_counter.inc();
        Ok(())
    }

    fn insert_token_account_all(&mut self) -> Result<()> {
        if self.queue_token_account.len() == 0 {
            return Ok(());
        }
        let mut params_prep: Vec<Box<dyn ToSql + Sync>> = Vec::new();
        let mut cnt = 0;
        for token_account in self.queue_token_account.drain(..) {
            params_prep.push(Box::new(pk(token_account.pubkey)));
            params_prep.push(Box::new(pk(token_account.mint)));
            params_prep.push(Box::new(pk(token_account.owner)));
            params_prep.push(Box::new(u64_sql(token_account.amount)));
            params_prep.push(Box::new(token_account.delegate.map(|t| pk(t))));
            params_prep.push(Box::new(token_account.state as i32));
            params_prep.push(Box::new(token_account.is_native.map(|t| u64_sql(t))));
            params_prep.push(Box::new(u64_sql(token_account.delegated_amount)));
            params_prep.push(Box::new(token_account.close_authority.map(|t| pk(t))));
            params_prep.push(Box::new(u64_sql(token_account.write_version)));
            cnt += 1;
        }
        let query = INSERT_TOKEN_ACCOUNT.replace("{}", &Self::generate_values(cnt, 10));
        Self::execute_with_retry(&mut self.db, query, params_prep, 1)?;
        Ok(())
    }

    fn insert_token_mint_all(&mut self) -> Result<()> {
        if self.queue_token_mint.len() == 0 {
            return Ok(());
        }
        let mut params_prep: Vec<Box<dyn ToSql + Sync>> = Vec::new();
        let mut cnt = 0;
        for token_mint in self.queue_token_mint.drain(..) {
            params_prep.push(Box::new(pk(token_mint.pubkey)));
            params_prep.push(Box::new(token_mint.mint_authority.map(|m| pk(m))));
            params_prep.push(Box::new(u64_sql(token_mint.supply)));
            params_prep.push(Box::new(token_mint.decimals as i32));
            params_prep.push(Box::new(token_mint.is_initialized));
            params_prep.push(Box::new(token_mint.freeze_authority.map(|t| pk(t))));
            params_prep.push(Box::new(u64_sql(token_mint.write_version)));
            cnt += 1;
        }
        let query = INSERT_TOKEN_MINT.replace("{}", &Self::generate_values(cnt, 7));
        Self::execute_with_retry(&mut self.db, query, params_prep, 1)?;
        Ok(())
    }

    fn insert_token_metadata(&mut self, account: &StoredAccountMeta) -> Result<()> {
        if account.data.is_empty() {
            return Ok(());
        }
        let mut data_peek = account.data;
        let account_key = match mpl_metadata::AccountKey::deserialize(&mut data_peek) {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };
        match account_key {
            mpl_metadata::AccountKey::MetadataV1 => {
                let meta_v1 = mpl_metadata::Metadata::deserialize(&mut data_peek).map_err(|e| {
                    format!(
                        "Invalid token-metadata v1 metadata acc {}: {}",
                        account.meta.pubkey, e
                    )
                })?;

                let meta_v1_1 = mpl_metadata::MetadataExt::deserialize(&mut data_peek).ok();
                let meta_v1_2 = meta_v1_1
                    .as_ref()
                    .and_then(|_| mpl_metadata::MetadataExtV1_2::deserialize(&mut data_peek).ok());
                let collection = meta_v1_2.as_ref().and_then(|m| m.collection.as_ref());

                let token_metadata = TokenMetadata {
                    pubkey: account.meta.pubkey,
                    mint: meta_v1.mint,
                    name: sanitize_str(meta_v1.data.name),
                    symbol: sanitize_str(meta_v1.data.symbol),
                    uri: sanitize_str(meta_v1.data.uri),
                    seller_fee_basis_points: meta_v1.data.seller_fee_basis_points,
                    primary_sale_happened: meta_v1.primary_sale_happened,
                    is_mutable: meta_v1.is_mutable,
                    edition_nonce: meta_v1_1.map(|c| c.edition_nonce).unwrap_or_default(),
                    collection_verified: collection.map(|c| c.verified),
                    collection_key: collection.map(|c| c.key),
                    write_version: account.meta.write_version,
                };
                self.queue_token_metadata.push(token_metadata);
                if self.queue_token_metadata.len() >= self.batch_size {
                    self.insert_token_metadata_all()?;
                }
            }
            _ => return Ok(()), // TODO
        }
        self.progress.metaplex_accounts_counter.inc();
        Ok(())
    }

    fn insert_token_metadata_all(&mut self) -> Result<()> {
        if self.queue_token_metadata.len() == 0 {
            return Ok(());
        }
        let mut params_prep: Vec<Box<dyn ToSql + Sync>> = Vec::new();
        let mut cnt = 0;
        for metadata in self.queue_token_metadata.drain(..) {
            params_prep.push(Box::new(pk(metadata.pubkey)));
            params_prep.push(Box::new(pk(metadata.mint)));
            params_prep.push(Box::new(metadata.name));
            params_prep.push(Box::new(metadata.symbol));
            params_prep.push(Box::new(metadata.uri));
            params_prep.push(Box::new(metadata.seller_fee_basis_points as i32));
            params_prep.push(Box::new(metadata.primary_sale_happened));
            params_prep.push(Box::new(metadata.is_mutable));
            params_prep.push(Box::new(metadata.edition_nonce.map(|e| e as i32)));
            params_prep.push(Box::new(metadata.collection_verified));
            params_prep.push(Box::new(metadata.collection_key.map(|t| pk(t))));
            params_prep.push(Box::new(u64_sql(metadata.write_version)));
            cnt += 1;
        }
        let query = INSERT_TOKEN_METADATA.replace("{}", &Self::generate_values(cnt, 12));
        Self::execute_with_retry(&mut self.db, query, params_prep, 1)?;
        Ok(())
    }

    fn insert_name_service_records(&mut self) -> Result<u64> {
        let records = self.name_service.get_records();
        let records_chunked: Vec<&[spl_name_service::Record]> =
            records.chunks(self.batch_size).collect();
        for chunk in records_chunked {
            self.insert_name_service_records_all(chunk)?;
        }
        Ok(records.len() as u64)
    }

    fn insert_name_service_records_all(
        &mut self,
        records: &[spl_name_service::Record],
    ) -> Result<()> {
        if records.len() == 0 {
            return Ok(());
        }
        let mut params_prep: Vec<Box<dyn ToSql + Sync>> = Vec::new();
        let mut cnt = 0;
        for record in records {
            params_prep.push(Box::new(pk(record.pubkey)));
            params_prep.push(Box::new(pk(record.owner)));
            params_prep.push(Box::new(record.record_type.clone()));
            params_prep.push(Box::new(record.record.clone()));
            params_prep.push(Box::new(u64_sql(record.write_version)));
            cnt += 1;
        }
        let query = INSERT_NAME_SERVICE.replace("{}", &Self::generate_values(cnt, 5));
        Self::execute_with_retry(&mut self.db, query, params_prep, 1)?;
        Ok(())
    }

    fn generate_values(records: usize, columns_per_record: usize) -> String {
        let mut rows = Vec::new();
        for i in 0..records {
            let mut row = Vec::new();
            for j in 0..columns_per_record {
                row.push(format!("${}", columns_per_record * i + j + 1));
            }
            rows.push(format!("({})", row.join(", ")));
        }
        return rows.join(", ");
    }

    fn execute_with_retry(
        db: &mut Client,
        query: String,
        params_raw: Vec<Box<dyn ToSql + Sync>>,
        retries_remaining: usize,
    ) -> Result<()> {
        let params: Vec<&(dyn ToSql + Sync)> = params_raw
            .iter()
            .map(|x| x.as_ref() as &(dyn ToSql + Sync))
            .collect();
        match db.execute(&query, &params) {
            Ok(_) => Ok(()),
            Err(e) => {
                if retries_remaining > 0 {
                    warn!("retrying pg err: {}", e);
                    // In practice, most errors will be race conditions (DEADLOCK) between insert
                    // batches with the same data. We sleep a random amount of time so they
                    // don't re-collide.
                    let mut rng = rand::thread_rng();
                    thread::sleep(time::Duration::from_millis(rng.gen_range(1..20)));
                    return Self::execute_with_retry(db, query, params_raw, retries_remaining - 1);
                }
                Err(Box::new(e))
            }
        }
    }
}

fn pk(pubkey: Pubkey) -> Vec<u8> {
    pubkey.to_bytes().to_vec()
}

fn u64_sql(u: u64) -> i64 {
    u as i64
}

fn sanitize_str(s: String) -> String {
    s.replace('\0', "")
}

struct ProgressCounter {
    progress_bar: Mutex<ProgressBar>,
    counter: AtomicU64,
}

impl ProgressCounter {
    fn new(progress_bar: ProgressBar) -> Self {
        Self {
            progress_bar: Mutex::new(progress_bar),
            counter: AtomicU64::new(0),
        }
    }

    fn get(&self) -> u64 {
        self.counter.load(Ordering::Relaxed)
    }

    fn inc(&self) {
        let count = self.counter.fetch_add(1, Ordering::Relaxed);
        if count % 1024 == 0 {
            self.progress_bar.lock().unwrap().set_position(count)
        }
    }
}

impl Drop for ProgressCounter {
    fn drop(&mut self) {
        let progress_bar = self.progress_bar.lock().unwrap();
        progress_bar.set_position(self.get());
        progress_bar.finish();
    }
}

const INSERT_ACCOUNT: &str = "
INSERT INTO account AS tbl (pubkey, data_len, owner, lamports, executable, rent_epoch, write_version)
VALUES {}
ON CONFLICT (pubkey) DO UPDATE SET
    data_len=excluded.data_len,
    owner=excluded.owner,
    lamports=excluded.lamports,
    executable=excluded.executable,
    rent_epoch=excluded.rent_epoch,
    write_version=excluded.write_version
WHERE tbl.write_version < excluded.write_version
";

const INSERT_TOKEN_ACCOUNT: &str = "
INSERT INTO token_account AS tbl (pubkey, mint, owner, amount, delegate, state, is_native, delegated_amount, close_authority, write_version)
VALUES {}
ON CONFLICT (pubkey) DO UPDATE SET
    pubkey=excluded.pubkey,
    mint=excluded.mint,
    owner=excluded.owner,
    amount=excluded.amount,
    delegate=excluded.delegate,
    state=excluded.state,
    is_native=excluded.is_native,
    delegated_amount=excluded.delegated_amount,
    close_authority=excluded.close_authority,
    write_version=excluded.write_version
WHERE tbl.write_version < excluded.write_version
";

const INSERT_TOKEN_MINT: &str = "
INSERT INTO token_mint AS tbl (pubkey, mint_authority, supply, decimals, is_initialized, freeze_authority, write_version)
VALUES {}
ON CONFLICT (pubkey) DO UPDATE SET
    pubkey=excluded.pubkey,
    mint_authority=excluded.mint_authority,
    supply=excluded.supply,
    decimals=excluded.decimals,
    is_initialized=excluded.is_initialized,
    freeze_authority=excluded.freeze_authority,
    write_version=excluded.write_version
WHERE tbl.write_version < excluded.write_version
";

const INSERT_TOKEN_METADATA: &str = "
INSERT INTO token_metadata AS tbl (pubkey, mint, name, symbol, uri, seller_fee_basis_points, primary_sale_happened, is_mutable, edition_nonce, collection_verified, collection_key, write_version)
VALUES {}
ON CONFLICT (pubkey) DO UPDATE SET
    pubkey=excluded.pubkey,
    mint=excluded.mint,
    name=excluded.name,
    symbol=excluded.symbol,
    uri=excluded.uri,
    seller_fee_basis_points=excluded.seller_fee_basis_points,
    primary_sale_happened=excluded.primary_sale_happened,
    is_mutable=excluded.is_mutable,
    edition_nonce=excluded.edition_nonce,
    collection_verified=excluded.collection_verified,
    collection_key=excluded.collection_key,
    write_version=excluded.write_version
WHERE tbl.write_version < excluded.write_version
";

const INSERT_NAME_SERVICE: &str = "
INSERT INTO name_service AS tbl (pubkey, owner, record_type, record, write_version)
VALUES {}
ON CONFLICT (pubkey) DO UPDATE SET
    pubkey=excluded.pubkey,
    owner=excluded.owner,
    record_type=excluded.record_type,
    record=excluded.record,
    write_version=excluded.write_version
WHERE tbl.write_version < excluded.write_version
";

const CREATE_TABLES: &str = "
CREATE TABLE IF NOT EXISTS account (
    pubkey BYTEA PRIMARY KEY,
    data_len BIGINT NOT NULL,
    owner BYTEA NOT NULL,
    lamports BIGINT NOT NULL,
    executable BOOLEAN NOT NULL,
    rent_epoch BIGINT NOT NULL,
    write_version BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS token_mint (
    pubkey BYTEA PRIMARY KEY,
    mint_authority BYTEA NULL,
    supply BIGINT NOT NULL,
    decimals INT NOT NULL,
    is_initialized BOOLEAN NOT NULL,
    freeze_authority BYTEA NULL,
    write_version BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS token_account (
    pubkey BYTEA PRIMARY KEY,
    mint BYTEA NOT NULL,
    owner BYTEA NOT NULL,
    amount BIGINT NOT NULL,
    delegate BYTEA,
    state INT NOT NULL,
    is_native BIGINT,
    delegated_amount BIGINT NOT NULL,
    close_authority BYTEA,
    write_version BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS token_metadata (
    pubkey BYTEA PRIMARY KEY,
    mint BYTEA NOT NULL,
    name TEXT NOT NULL,
    symbol TEXT NOT NULL,
    uri TEXT NOT NULL,
    seller_fee_basis_points INT NOT NULL,
    primary_sale_happened BOOLEAN NOT NULL,
    is_mutable BOOLEAN NOT NULL,
    edition_nonce INT,
    collection_verified BOOLEAN,
    collection_key BYTEA,
    write_version BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS name_service (
    pubkey BYTEA PRIMARY KEY,
    owner BYTEA NOT NULL,
    record_type TEXT NOT NULL,
    record TEXT NOT NULL,
    write_version BIGINT NOT NULL
);
";
