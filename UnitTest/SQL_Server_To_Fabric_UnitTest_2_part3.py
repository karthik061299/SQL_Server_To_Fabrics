# Test fixtures
@pytest.fixture
def mock_fabric_connection():
    """Create a mock Fabric SQL connection"""
    return MockFabricConnection()

@pytest.fixture
def setup_operation_log_table(mock_fabric_connection):
    """Set up the operation_log table"""
    mock_fabric_connection.tables['operation_log'] = pd.DataFrame({
        'log_id': [],
        'operation_name': [],
        'status': [],
        'record_count': [],
        'message': [],
        'error_message': [],
        'timestamp': []
    })
    return mock_fabric_connection

@pytest.fixture
def setup_employee_table(mock_fabric_connection):
    """Set up the Employee table with test data"""
    mock_fabric_connection.tables['Employee'] = pd.DataFrame({
        'EmployeeNo': [1, 2, 3, 4, 5],
        'FirstName': ['John', 'Jane', 'Bob', 'Alice', 'Charlie'],
        'LastName': ['Doe', 'Smith', 'Johnson', 'Brown', 'Davis'],
        'DepartmentNo': [1, 2, 1, 3, 2],
        'HireDate': [datetime.now()] * 5,
        'test_case': ['normal'] * 5
    })
    return mock_fabric_connection

@pytest.fixture
def setup_salary_table(mock_fabric_connection):
    """Set up the Salary table with test data"""
    mock_fabric_connection.tables['Salary'] = pd.DataFrame({
        'EmployeeNo': [1, 2, 3, 4, 5],
        'NetPay': [50000, 60000, 55000, 65000, 70000],
        'LastUpdate': [datetime.now()] * 5
    })
    return mock_fabric_connection

@pytest.fixture
def setup_empty_employee_table(mock_fabric_connection):
    """Set up an empty Employee table"""
    mock_fabric_connection.tables['Employee'] = pd.DataFrame({
        'EmployeeNo': [],
        'FirstName': [],
        'LastName': [],
        'DepartmentNo': [],
        'HireDate': [],
        'test_case': ['empty_source']
    })
    return mock_fabric_connection

@pytest.fixture
def setup_null_values_employee_table(mock_fabric_connection):
    """Set up Employee table with NULL values"""
    mock_fabric_connection.tables['Employee'] = pd.DataFrame({
        'EmployeeNo': [1, 2, 3, 4, None],
        'FirstName': ['John', None, 'Bob', 'Alice', 'Charlie'],
        'LastName': ['Doe', 'Smith', None, 'Brown', 'Davis'],
        'DepartmentNo': [1, None, 1, 3, 2],
        'HireDate': [datetime.now(), None, datetime.now(), datetime.now(), datetime.now()],
        'test_case': ['null_values'] * 5
    })
    return mock_fabric_connection

@pytest.fixture
def setup_whitespace_employee_table(mock_fabric_connection):
    """Set up Employee table with whitespace in string fields"""
    mock_fabric_connection.tables['Employee'] = pd.DataFrame({
        'EmployeeNo': [1, 2, 3],
        'FirstName': ['  John  ', 'Jane', '  Bob '],
        'LastName': ['Doe  ', '  Smith', '  Johnson  '],
        'DepartmentNo': [1, 2, 1],
        'HireDate': [datetime.now()] * 3,
        'test_case': ['whitespace'] * 3
    })
    return mock_fabric_connection