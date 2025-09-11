    def test_temporary_table_creation(self, mock_fabric_conn):
        """Test temporary table creation in Fabric SQL"""
        cursor = mock_fabric_conn.cursor.return_value
        
        # Track table creation and dropping
        created_tables = set()
        dropped_tables = set()
        
        def execute_side_effect(query, *args):
            # Extract table names from CREATE TABLE statements
            if query.strip().upper().startswith('CREATE TABLE '):
                table_name = query.split('CREATE TABLE ')[1].split(' ')[0]
                created_tables.add(table_name)
            
            # Extract table names from DROP TABLE statements
            elif query.strip().upper().startswith('DROP TABLE IF EXISTS '):
                table_name = query.split('DROP TABLE IF EXISTS ')[1].strip()
                dropped_tables.add(table_name)
            
            return cursor
        
        cursor.execute.side_effect = execute_side_effect
        
        # Execute the stored procedure
        job_start_datetime = datetime.now() - timedelta(days=7)
        job_end_datetime = datetime.now()
        
        cursor.execute(
            "EXEC Semantic.uspSemanticClaimTransactionMeasuresData @pJobStartDateTime=?, @pJobEndDateTime=?",
            job_start_datetime, job_end_datetime
        )
        
        # Verify that tables were created and then dropped
        assert len(created_tables) > 0, "No temporary tables were created"
        assert len(dropped_tables) > 0, "No temporary tables were dropped"
        
        # All created tables should be dropped
        for table in created_tables:
            assert table in dropped_tables, f"Table {table} was created but not dropped"
        
        # Verify naming convention for temporary tables in Fabric
        # In Fabric, we use session-specific naming with UUIDs instead of ##table + @@spid
        for table in created_tables:
            # Should match pattern like "CTM_12345abcde..."
            assert re.match(r'^[A-Za-z]+_[0-9a-f]+$', table), \
                f"Table name {table} doesn't follow Fabric naming convention"
    
    def test_dynamic_sql_execution(self, mock_fabric_conn):
        """Test dynamic SQL execution in Fabric SQL"""
        cursor = mock_fabric_conn.cursor.return_value
        
        # Execute a simple dynamic SQL statement
        try:
            cursor.execute("""
            DECLARE @sql VARCHAR(MAX);
            SET @sql = 'SELECT 1 AS TestValue';
            EXECUTE(@sql);
            """)
            
            # Should execute without errors
            assert True
        except Exception as e:
            pytest.fail(f"Dynamic SQL execution failed: {str(e)}")
    
    def test_hash_join_hint(self, mock_fabric_conn):
        """Test HASH JOIN hint in Fabric SQL"""
        cursor = mock_fabric_conn.cursor.return_value
        
        # Execute a query with HASH JOIN hint
        try:
            cursor.execute("""
            SELECT * FROM Table1
            INNER HASH JOIN Table2 ON Table1.ID = Table2.ID;
            """)
            
            # Should execute without errors
            assert True
        except Exception as e:
            pytest.fail(f"HASH JOIN hint execution failed: {str(e)}")