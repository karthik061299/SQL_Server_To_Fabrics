-- Create database
CREATE DATABASE InventoryManagement;
GO

-- Use the newly created database
USE InventoryManagement;
GO

-- Create a table for items
CREATE TABLE Items (
    ItemID INT IDENTITY(1,1) PRIMARY KEY,
    ItemName NVARCHAR(100) NOT NULL,
    ItemCategory NVARCHAR(50),
    UnitPrice DECIMAL(10, 2) NOT NULL,
    QuantityInStock INT NOT NULL,
    ReorderLevel INT NOT NULL,
    CreatedDate DATETIME DEFAULT GETDATE()
);
GO

-- Create a table for suppliers
CREATE TABLE Suppliers (
    SupplierID INT IDENTITY(1,1) PRIMARY KEY,
    SupplierName NVARCHAR(100) NOT NULL,
    ContactNumber NVARCHAR(15),
    Email NVARCHAR(100),
    Address NVARCHAR(200),
    CreatedDate DATETIME DEFAULT GETDATE()
);
GO

-- Create a table for purchase orders
CREATE TABLE PurchaseOrders (
    PurchaseOrderID INT IDENTITY(1,1) PRIMARY KEY,
    SupplierID INT NOT NULL FOREIGN KEY REFERENCES Suppliers(SupplierID),
    OrderDate DATETIME DEFAULT GETDATE(),
    TotalAmount DECIMAL(10, 2),
    Status NVARCHAR(20) DEFAULT 'Pending'
);
GO

-- Create a table for purchase order details
CREATE TABLE PurchaseOrderDetails (
    PODetailID INT IDENTITY(1,1) PRIMARY KEY,
    PurchaseOrderID INT NOT NULL FOREIGN KEY REFERENCES PurchaseOrders(PurchaseOrderID),
    ItemID INT NOT NULL FOREIGN KEY REFERENCES Items(ItemID),
    Quantity INT NOT NULL,
    UnitPrice DECIMAL(10, 2) NOT NULL
);
GO

-- Create a table for sales
CREATE TABLE Sales (
    SaleID INT IDENTITY(1,1) PRIMARY KEY,
    SaleDate DATETIME DEFAULT GETDATE(),
    TotalAmount DECIMAL(10, 2),
    CustomerName NVARCHAR(100),
    Status NVARCHAR(20) DEFAULT 'Completed'
);
GO

-- Create a table for sales details
CREATE TABLE SaleDetails (
    SaleDetailID INT IDENTITY(1,1) PRIMARY KEY,
    SaleID INT NOT NULL FOREIGN KEY REFERENCES Sales(SaleID),
    ItemID INT NOT NULL FOREIGN KEY REFERENCES Items(ItemID),
    Quantity INT NOT NULL,
    UnitPrice DECIMAL(10, 2) NOT NULL
);
GO

-- Insert sample data into Items
INSERT INTO Items (ItemName, ItemCategory, UnitPrice, QuantityInStock, ReorderLevel)
VALUES 
('Laptop', 'Electronics', 800.00, 50, 10),
('Smartphone', 'Electronics', 500.00, 100, 20),
('Desk Chair', 'Furniture', 120.00, 40, 5),
('Notebook', 'Stationery', 5.00, 200, 50);
GO

-- Insert sample data into Suppliers
INSERT INTO Suppliers (SupplierName, ContactNumber, Email, Address)
VALUES 
('Tech Supplier Co.', '123-456-7890', 'contact@techsupplier.com', '123 Tech Street'),
('Furniture Supplies Ltd.', '987-654-3210', 'sales@furnituresupplies.com', '456 Furniture Avenue');
GO

-- Insert a sample purchase order
INSERT INTO PurchaseOrders (SupplierID, TotalAmount, Status)
VALUES (1, 5000.00, 'Completed');
GO

-- Insert details for the purchase order
INSERT INTO PurchaseOrderDetails (PurchaseOrderID, ItemID, Quantity, UnitPrice)
VALUES 
(1, 1, 10, 800.00),
(1, 2, 5, 500.00);
GO

-- Insert a sample sale
INSERT INTO Sales (SaleDate, TotalAmount, CustomerName)
VALUES (GETDATE(), 1500.00, 'John Doe');
GO

-- Insert details for the sale
INSERT INTO SaleDetails (SaleID, ItemID, Quantity, UnitPrice)
VALUES 
(1, 1, 2, 800.00),
(1, 2, 1, 500.00);
GO

-- Update stock levels after sales
UPDATE Items
SET QuantityInStock = QuantityInStock - sd.Quantity
FROM Items i
JOIN SaleDetails sd ON i.ItemID = sd.ItemID;
GO

-- Inventory report
SELECT 
    ItemName,
    ItemCategory,
    UnitPrice,
    QuantityInStock,
    ReorderLevel,
    CASE 
        WHEN QuantityInStock <= ReorderLevel THEN 'Reorder Needed'
        ELSE 'Sufficient Stock'
    END AS StockStatus
FROM Items;
GO
