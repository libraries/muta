use std::fs;
use std::sync::Arc;
use std::time::SystemTime;

use byteorder::{BigEndian, ByteOrder};
use protocol::codec::ProtocolCodec;
use protocol::fixed_codec::FixedCodec;
use protocol::traits::Storage;
use protocol::types::Hash;

use crate::adapter::memory::MemoryAdapter;
use crate::adapter::rocks::{Config, RocksAdapter};
use crate::tests::{get_random_bytes, mock_block, mock_proof, mock_receipt, mock_signed_tx};
use crate::ImplStorage;

#[test]
fn test_storage_block_insert() {
    let storage = ImplStorage::new(Arc::new(MemoryAdapter::new()));

    let height = 100;
    let block = mock_block(height, Hash::digest(get_random_bytes(10)));
    let block_hash = Hash::digest(block.encode_fixed().unwrap());

    exec!(storage.insert_block(block));

    let block = exec!(storage.get_latest_block());
    assert_eq!(height, block.header.height);

    let block = exec!(storage.get_block_by_height(height));
    assert_eq!(height, block.header.height);

    let block = exec!(storage.get_block_by_hash(block_hash));
    assert_eq!(height, block.header.height);
}

#[test]
fn test_storage_receipts_insert() {
    let storage = ImplStorage::new(Arc::new(MemoryAdapter::new()));

    let mut receipts = Vec::new();
    let mut hashes = Vec::new();

    for _ in 0..10 {
        let tx_hash = Hash::digest(get_random_bytes(10));
        hashes.push(tx_hash.clone());
        let receipt = mock_receipt(tx_hash.clone());
        receipts.push(receipt);
    }

    exec!(storage.insert_receipts(receipts.clone()));
    let receipts_2 = exec!(storage.get_receipts(hashes));

    for i in 0..10 {
        assert_eq!(
            receipts.get(i).unwrap().tx_hash,
            receipts_2.get(i).unwrap().tx_hash
        );
    }
}

#[test]
fn test_storage_transactions_insert() {
    let storage = ImplStorage::new(Arc::new(MemoryAdapter::new()));

    let mut transactions = Vec::new();
    let mut hashes = Vec::new();

    for _ in 0..10 {
        let tx_hash = Hash::digest(get_random_bytes(10));
        hashes.push(tx_hash.clone());
        let transaction = mock_signed_tx(tx_hash.clone());
        transactions.push(transaction);
    }

    exec!(storage.insert_transactions(transactions.clone()));
    let transactions_2 = exec!(storage.get_transactions(hashes));

    for i in 0..10 {
        assert_eq!(
            transactions.get(i).unwrap().tx_hash,
            transactions_2.get(i).unwrap().tx_hash
        );
    }
}

#[test]
fn test_storage_latest_proof_insert() {
    let storage = ImplStorage::new(Arc::new(MemoryAdapter::new()));

    let block_hash = Hash::digest(get_random_bytes(10));
    let proof = mock_proof(block_hash);

    exec!(storage.update_latest_proof(proof.clone()));
    let proof_2 = exec!(storage.get_latest_proof());

    assert_eq!(proof.block_hash, proof_2.block_hash);
}

#[test]
fn test_storage_wal_insert() {
    let storage = ImplStorage::new(Arc::new(MemoryAdapter::new()));

    let info = get_random_bytes(64);
    exec!(storage.update_overlord_wal(info.clone()));
    let info_2 = exec!(storage.load_overlord_wal());
    assert_eq!(info, info_2);
}

#[test]
fn test_storage_stat() {
    fs::remove_dir_all("rocksdb/test_adapter_stat").unwrap();
    let adapter = Arc::new(
        RocksAdapter::new("rocksdb/test_adapter_stat".to_string(), Config::default()).unwrap(),
    );
    let column = adapter.db.cf_handle("c2").unwrap();
    for i in 0..100000 {
        let height = i / 12000;
        let block_index = i % 12000;

        let mut buf = [0; 4];
        let mut real_key: Vec<u8> = Vec::new();
        real_key.push(97);
        BigEndian::write_u32(&mut buf, height);
        for i in 0..4 {
            real_key.push(buf[i]);
        }
        BigEndian::write_u32(&mut buf, block_index);
        for i in 0..4 {
            real_key.push(buf[i]);
        }
        let tx_hash = Hash::digest(get_random_bytes(10));
        let k: &[u8] = &tx_hash.as_bytes()[..];
        for i in 0..k.len() {
            real_key.push(k[i]);
        }

        let mut transaction = mock_signed_tx(tx_hash.clone());
        adapter
            .db
            .put_cf(column, real_key, exec!(transaction.encode()).to_vec()).unwrap();
    }

    let mut iter = adapter.db.raw_iterator_cf(column).unwrap();
    let mut collect = vec![];
    let now_scan = SystemTime::now();
    iter.seek(vec![97, 0, 0, 0, 0]);
    while iter.valid() {
        let k = iter.key().unwrap();
        if k[4] != 0 {
            break
        }
        collect.push(k);
        iter.next();
    }
    println!(
        "scan {:?} tx spent {:?}ms",
        collect.len(),
        now_scan.elapsed().unwrap().as_millis()
    );

    // let storage = Arc::new(ImplStorage::new(Arc::clone(&adapter)));

    // let loop_num = 10;
    // let size = 1_000_000;
    // let rand_size = 500; // 500 * 10 = 5000

    // let mut head_5000_hashes = Vec::new();
    // let mut tail_5000_hashes = Vec::new();
    // let mut rand_5000_hashes = Vec::new();

    // for i in 0..loop_num {
    //     let mut transactions = Vec::new();
    //     let mut hashes = Vec::new();

    //     for _ in 0..size {
    //         let tx_hash = Hash::digest(get_random_bytes(10));
    //         hashes.push(tx_hash.clone());
    //         let transaction = mock_signed_tx(tx_hash.clone());
    //         transactions.push(transaction);
    //     }
    //     if i == 0 {
    //         head_5000_hashes = hashes[0..5000].to_vec();
    //     }
    //     if i == loop_num - 1 {
    //         tail_5000_hashes = hashes[size - 5000..size].to_vec();
    //     }
    //     rand_5000_hashes.extend_from_slice(&hashes[0..rand_size]);

    //     let now = SystemTime::now();
    //     exec!(storage.insert_transactions(transactions.clone()));
    //     println!(
    //         "insert {:?} tx spent {:?}ms",
    //         size,
    //         now.elapsed().unwrap().as_millis()
    //     );
    // }

    // let now_head = SystemTime::now();
    // let r = exec!(storage.get_transactions(head_5000_hashes.to_vec()));
    // println!(
    //     "get head {:?} tx spent {:?}ms", r.len(),
    //     now_head.elapsed().unwrap().as_millis()
    // );

    // let column = adapter.db.cf_handle("c2").unwrap();
    // let mut iter = adapter.db.raw_iterator_cf(column).unwrap();
    // let mut collect= vec![];
    // let now_scan = SystemTime::now();

    // iter.seek(b"b");
    // while iter.valid() {
    //     collect.push(iter.key());
    //     iter.next();
    // }
    // println!(
    //     "scan {:?} tx spent {:?}ms", collect.len(),
    //     now_scan.elapsed().unwrap().as_millis()
    // );

    // let column = adapter.db.cf_handle("c2").unwrap();
    // let iter = adapter.db.iterator_cf(column,
    // rocksdb::IteratorMode::From(b"aaaa", rocksdb::Direction::Forward)
    // ).unwrap(); let mut collect= vec![];
    // let now_scan = SystemTime::now();
    // for (key, value) in iter {
    //     collect.push(key);
    // }
    // println!(
    //     "scan {:?} tx spent {:?}ms", collect.len(),
    //     now_scan.elapsed().unwrap().as_millis()
    // );

    // let now_tail = SystemTime::now();
    // exec!(storage.get_transactions(tail_5000_hashes.to_vec()));
    // println!(
    //     "get tail 5000 tx spent {:?}ms",
    //     now_tail.elapsed().unwrap().as_millis()
    // );

    // let now_rand = SystemTime::now();
    // exec!(storage.get_transactions(rand_5000_hashes.to_vec()));
    // println!(
    //     "get rand 5000 tx spent {:?}ms",
    //     now_rand.elapsed().unwrap().as_millis()
    // );
}
