import fs from 'fs';
import csv from 'csv-parser';
import moment from 'moment';
import { createObjectCsvWriter as createCsvWriter } from 'csv-writer';
import { balanceRange } from './config.js';

const excludedWallets = [
    '0x54945180db7943c0ed0fee7edab2bd24620256bc',
    '0x1bee69b7dfffa4e2d53c2a2df135c388ad25dcd2',
    '0x93c4b944d05dfe6df7645a86cd2206016c51564d',
    '0x0fe4f44bee93503346a3ac9ee5a26b130a5796d6'
];

function processFile(inputFilePath, outputFilePath) {
    const wallets = {};
    let countInRange = 0;

    fs.createReadStream(inputFilePath)
        .pipe(csv())
        .on('data', (row) => {
            const from = row['From'];
            const to = row['To'];
            const value = parseFloat(row['TokenValue']);
            const date = moment(row['DateTime (UTC)'], 'YYYY-MM-DD HH:mm:ss');

            if (from && !excludedWallets.includes(from)) {
                wallets[from] = wallets[from] || { balance: 0, firstTx: date, lastTx: date };
                wallets[from].balance += value;
                wallets[from].lastTx = moment.max(wallets[from].lastTx, date);
                wallets[from].firstTx = moment.min(wallets[from].firstTx, date);
            }

            if (to && !excludedWallets.includes(to)) {
                wallets[to] = wallets[to] || { balance: 0, firstTx: date, lastTx: date };
                wallets[to].balance -= value;
                wallets[to].lastTx = moment.max(wallets[to].lastTx, date);
                wallets[to].firstTx = moment.min(wallets[to].firstTx, date);
            }
        })
        .on('end', () => {
            const csvWriter = createCsvWriter({
                path: outputFilePath,
                header: [
                    { id: 'address', title: 'Address' },
                    { id: 'balance', title: 'Balance' },
                    { id: 'firstTx', title: 'First Transaction (UTC)' },
                    { id: 'lastTx', title: 'Last Transaction (UTC)' }
                ]
            });

            const records = Object.entries(wallets).map(([address, data]) => ({
                address,
                balance: data.balance,
                firstTx: data.firstTx.format('YYYY-MM-DD HH:mm:ss'),
                lastTx: data.lastTx.format('YYYY-MM-DD HH:mm:ss')
            }));

            csvWriter.writeRecords(records)
                .then(() => console.log(`Data written to ${outputFilePath} successfully.`));

            Object.values(wallets).forEach(wallet => {
                if (wallet.balance >= balanceRange.min && wallet.balance <= balanceRange.max) {
                    countInRange++;
                }
            });

            console.log(`File: ${outputFilePath} - Wallets in range [${balanceRange.min}, ${balanceRange.max}]: ${countInRange}`);
        });
}

const filePaths = [
    { input: './raw_data/raw-cbETH.csv', output: './result/cbETH.csv' },
    { input: './raw_data/raw-rETH.csv', output: './result/rETH.csv' },
    { input: './raw_data/raw-stETH.csv', output: './result/stETH.csv' },
    { input: './raw_data/raw-swETH.csv', output: './result/swETH.csv' }
];

filePaths.forEach(({ input, output }) => processFile(input, output));