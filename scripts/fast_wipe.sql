-- Fast Data Wipe - Direct SQL TRUNCATE
-- FASTEST method to wipe all ERPNext transactional data
--
-- WARNING: DESTRUCTIVE and IRREVERSIBLE - Backup first!
--
-- Execute:
--   docker exec -i soundboxstore-erpnext-db-1 \
--     mysql -u root -padmin -D _fa4c6a4bad15d5ed < scripts/fast_wipe.sql
--
-- Or interactively:
--   docker exec -it soundboxstore-erpnext-db-1 \
--     mysql -u root -padmin -D _fa4c6a4bad15d5ed
--   source /path/to/fast_wipe.sql

-- Disable foreign key checks to allow truncation
SET FOREIGN_KEY_CHECKS = 0;

SELECT '========================================' AS '';
SELECT 'Starting Fast Data Wipe...' AS '';
SELECT '========================================' AS '';

-- 1. System documents (must go first - these are auto-generated)
SELECT 'Truncating Stock Ledger Entry...' AS '';
TRUNCATE TABLE `tabStock Ledger Entry`;

SELECT 'Truncating GL Entry...' AS '';
TRUNCATE TABLE `tabGL Entry`;

SELECT 'Truncating Serial and Batch Bundle...' AS '';
TRUNCATE TABLE `tabSerial and Batch Bundle`;

-- 2. Transaction child tables (delete before parent)
SELECT 'Truncating transaction child tables...' AS '';
TRUNCATE TABLE `tabDelivery Note Item`;
TRUNCATE TABLE `tabSales Order Item`;
TRUNCATE TABLE `tabPurchase Receipt Item`;
TRUNCATE TABLE `tabPurchase Order Item`;
TRUNCATE TABLE `tabStock Entry Detail`;
TRUNCATE TABLE `tabSales Invoice Item`;
TRUNCATE TABLE `tabPurchase Invoice Item`;
TRUNCATE TABLE `tabStock Reconciliation Item`;

-- 3. Transaction parent tables
SELECT 'Truncating Delivery Note...' AS '';
TRUNCATE TABLE `tabDelivery Note`;

SELECT 'Truncating Sales Invoice...' AS '';
TRUNCATE TABLE `tabSales Invoice`;

SELECT 'Truncating Sales Order...' AS '';
TRUNCATE TABLE `tabSales Order`;

SELECT 'Truncating Purchase Receipt...' AS '';
TRUNCATE TABLE `tabPurchase Receipt`;

SELECT 'Truncating Purchase Invoice...' AS '';
TRUNCATE TABLE `tabPurchase Invoice`;

SELECT 'Truncating Purchase Order...' AS '';
TRUNCATE TABLE `tabPurchase Order`;

SELECT 'Truncating Stock Entry...' AS '';
TRUNCATE TABLE `tabStock Entry`;

SELECT 'Truncating Stock Reconciliation...' AS '';
TRUNCATE TABLE `tabStock Reconciliation`;

-- 4. Custom DocTypes
SELECT 'Truncating Container Pre-Allocation...' AS '';
TRUNCATE TABLE `tabContainer Pre-Allocation`;

SELECT 'Truncating Container...' AS '';
TRUNCATE TABLE `tabContainer`;

-- 5. Optional: Master data (UNCOMMENT to wipe)
-- WARNING: This will delete all customers, items, addresses, contacts

-- SELECT 'Deleting Dynamic Links...' AS '';
-- TRUNCATE TABLE `tabDynamic Link`;

-- SELECT 'Deleting Addresses...' AS '';
-- TRUNCATE TABLE `tabAddress`;

-- SELECT 'Deleting Contacts...' AS '';
-- TRUNCATE TABLE `tabContact`;

-- SELECT 'Deleting Customers (except Guest)...' AS '';
-- DELETE FROM `tabCustomer` WHERE name != 'Guest';

-- SELECT 'Deleting Suppliers...' AS '';
-- TRUNCATE TABLE `tabSupplier`;

-- SELECT 'Deleting Items (stock items only)...' AS '';
-- DELETE FROM `tabItem` WHERE is_stock_item = 1;
-- DELETE FROM `tabItem Price` WHERE item_code IN (SELECT name FROM `tabItem` WHERE is_stock_item = 1);

-- Re-enable foreign key checks
SET FOREIGN_KEY_CHECKS = 1;

-- Verification: Count remaining documents
SELECT '========================================' AS '';
SELECT 'Data Wipe Complete - Verification' AS '';
SELECT '========================================' AS '';

SELECT 'Stock Ledger Entry' AS DocType, COUNT(*) AS Remaining FROM `tabStock Ledger Entry`
UNION ALL
SELECT 'GL Entry', COUNT(*) FROM `tabGL Entry`
UNION ALL
SELECT 'Delivery Note', COUNT(*) FROM `tabDelivery Note`
UNION ALL
SELECT 'Sales Invoice', COUNT(*) FROM `tabSales Invoice`
UNION ALL
SELECT 'Sales Order', COUNT(*) FROM `tabSales Order`
UNION ALL
SELECT 'Purchase Receipt', COUNT(*) FROM `tabPurchase Receipt`
UNION ALL
SELECT 'Purchase Invoice', COUNT(*) FROM `tabPurchase Invoice`
UNION ALL
SELECT 'Purchase Order', COUNT(*) FROM `tabPurchase Order`
UNION ALL
SELECT 'Stock Entry', COUNT(*) FROM `tabStock Entry`
UNION ALL
SELECT 'Container Pre-Allocation', COUNT(*) FROM `tabContainer Pre-Allocation`
UNION ALL
SELECT 'Container', COUNT(*) FROM `tabContainer`
UNION ALL
SELECT 'Customer', COUNT(*) FROM `tabCustomer`
UNION ALL
SELECT 'Item', COUNT(*) FROM `tabItem`;

SELECT '========================================' AS '';
SELECT 'If all counts are 0 (except Customer, Item if preserved), wipe successful!' AS '';
SELECT '========================================' AS '';
