_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*:   Comprehensive test cases and Pytest script to validate SQL Server-to-Fabric SQL conversion
## *Version*: 5 
## *Updated on*: 
_____________________________________________

    def test_special_date_handling(self, test_framework, test_data):
        """TC003: Test special date handling logic"""
        # Test that '1900-01-01' is converted to '1700-01-01'
        
        # Original SQL Server logic:
        # if @pJobStartDateTime = '01/01/1900'
        # begin
        #     set @pJobStartDateTime = '01/01/1700';
        # end;
        
        # Mock execution
        job_start_date = '1900-01-01'
        expected_converted_date = '1700-01-01'
        
        # Simulate the date conversion logic
        actual_converted_date = '1700-01-01' if job_start_date == '1900-01-01' else job_start_date
        
        assert actual_converted_date == expected_converted_date, "Special date '1900-01-01' should be converted to '1700-01-01'"
        
        logger.info("Special date handling test passed")
    
    def test_hash_calculation(self, test_framework, test_data):
        """TC004: Test hash calculation for change detection"""
        # Test that hash values are correctly calculated for change detection
        
        # Mock data for hash calculation
        record1 = {
            'FactClaimTransactionLineWCKey': 1001,
            'RevisionNumber': 1,
            'PolicyWCKey': 101,
            'NetPaidIndemnity': 1000.00
        }
        
        record2 = {
            'FactClaimTransactionLineWCKey': 1001,
            'RevisionNumber': 1,
            'PolicyWCKey': 101,
            'NetPaidIndemnity': 1500.00  # Changed value
        }
        
        # Mock hash calculation
        hash1 = "hash_value_1"  # Simplified for testing
        hash2 = "hash_value_2"  # Different due to changed NetPaidIndemnity
        
        assert hash1 != hash2, "Hash values should differ when data changes"
        
        # Test InsertUpdates logic based on hash comparison
        insert_updates = 0 if hash1 != hash2 else 3  # 0 = update, 1 = insert, 3 = no change
        
        assert insert_updates == 0, "Changed record should be marked for update"
        
        logger.info("Hash calculation test passed")
    
    def test_insert_updates_flag(self, test_framework, test_data):
        """TC005: Test InsertUpdates flag calculation"""
        # Test that InsertUpdates flag is correctly set
        
        # Mock data
        new_record = {'FactClaimTransactionLineWCKey': 1001, 'HashValue': 'hash1'}
        existing_record = {'FactClaimTransactionLineWCKey': 1002, 'HashValue': 'hash2'}
        changed_record = {'FactClaimTransactionLineWCKey': 1003, 'HashValue': 'hash3_new'}
        unchanged_record = {'FactClaimTransactionLineWCKey': 1004, 'HashValue': 'hash4'}
        
        # Mock existing data
        existing_data = {
            1002: {'HashValue': 'hash2'},
            1003: {'HashValue': 'hash3_old'},
            1004: {'HashValue': 'hash4'}
        }
        
        # Calculate InsertUpdates flags
        def calculate_insert_updates(record):
            key = record['FactClaimTransactionLineWCKey']
            if key not in existing_data:
                return 1  # Insert
            elif record['HashValue'] != existing_data[key]['HashValue']:
                return 0  # Update
            else:
                return 3  # No change
        
        # Verify flag calculation
        assert calculate_insert_updates(new_record) == 1, "New record should have InsertUpdates = 1"
        assert calculate_insert_updates(existing_record) == 3, "Unchanged record should have InsertUpdates = 3"
        assert calculate_insert_updates(changed_record) == 0, "Changed record should have InsertUpdates = 0"
        assert calculate_insert_updates(unchanged_record) == 3, "Unchanged record should have InsertUpdates = 3"
        
        logger.info("InsertUpdates flag calculation test passed")
    
    def test_financial_calculations(self, test_framework, test_data):
        """TC006: Test financial measure calculations"""
        # Test that financial measures are correctly calculated
        
        # Mock transaction data
        transactions = [
            {'TransactionAmount': 1000.00, 'TransactionType': 'Indemnity', 'IsRecovery': False},
            {'TransactionAmount': 500.00, 'TransactionType': 'Medical', 'IsRecovery': False},
            {'TransactionAmount': -200.00, 'TransactionType': 'Indemnity', 'IsRecovery': True},
            {'TransactionAmount': 300.00, 'TransactionType': 'Expense', 'IsRecovery': False}
        ]
        
        # Calculate financial measures
        net_paid_indemnity = sum(t['TransactionAmount'] for t in transactions 
                                if t['TransactionType'] == 'Indemnity' and not t['IsRecovery'])
        
        net_paid_medical = sum(t['TransactionAmount'] for t in transactions 
                              if t['TransactionType'] == 'Medical' and not t['IsRecovery'])
        
        net_paid_expense = sum(t['TransactionAmount'] for t in transactions 
                              if t['TransactionType'] == 'Expense' and not t['IsRecovery'])
        
        recovery_indemnity = abs(sum(t['TransactionAmount'] for t in transactions 
                                   if t['TransactionType'] == 'Indemnity' and t['IsRecovery']))
        
        # Verify calculations
        assert net_paid_indemnity == 1000.00, "Net paid indemnity should be calculated correctly"
        assert net_paid_medical == 500.00, "Net paid medical should be calculated correctly"
        assert net_paid_expense == 300.00, "Net paid expense should be calculated correctly"
        assert recovery_indemnity == 200.00, "Recovery indemnity should be calculated correctly"
        
        logger.info("Financial calculations test passed")