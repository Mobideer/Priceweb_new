import unittest
import os
import sqlite3
import json
from app import app
import db
import worker

class TestPriceWebSanity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Use a temporary database for testing
        cls.test_db = "test_priceweb.db"
        os.environ["PRICE_DB_PATH"] = cls.test_db
        db.DB_PATH = cls.test_db
        db.ensure_schema()

    @classmethod
    def tearDownClass(cls):
        # Cleanup test database
        if os.path.exists(cls.test_db):
            os.remove(cls.test_db)
        if os.path.exists(cls.test_db + "-wal"):
            os.remove(cls.test_db + "-wal")
        if os.path.exists(cls.test_db + "-shm"):
            os.remove(cls.test_db + "-shm")

    def test_db_connection(self):
        """Test database connection and basic schema."""
        conn = db.get_connection()
        self.assertIsInstance(conn, sqlite3.Connection)
        res = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='items_latest'").fetchone()
        self.assertIsNotNone(res)
        conn.close()

    def test_db_status(self):
        """Test db.get_db_status function."""
        status = db.get_db_status()
        self.assertTrue(status['ok'])
        self.assertEqual(status['items_db'], 0)
        self.assertEqual(status['db_path'], self.test_db)

    def test_worker_processing(self):
        """Test worker logic for processing a single product."""
        mock_product = {
            'sku': 'TEST-SKU-1',
            'name': 'Test Product',
            'price': 100.0,
            'quantity': 10,
            'suppliers': [
                {
                    'name': 'Supplier A',
                    'product': {
                        'price': 80.0,
                        'quantity': 5,
                        'currency': 'RUB',
                        'sku': 'SUP-A-SKU'
                    }
                },
                {
                    'name': 'Мой Склад',
                    'product': {
                        'price': 90.0,
                        'quantity': 2,
                        'currency': 'RUB'
                    }
                }
            ]
        }
        rates = {"USD": 90.0, "EUR": 100.0, "RUB": 1.0}
        result = worker.process_single_product(mock_product, rates)
        
        self.assertIsNotNone(result)
        self.assertEqual(result['sku'], 'TEST-SKU-1')
        self.assertEqual(result['our_price'], 100.0)
        self.assertEqual(result['my_sklad_price'], 90.0)
        self.assertEqual(result['min_sup_price'], 80.0)
        self.assertEqual(result['min_sup_supplier'], 'Supplier A')
        self.assertEqual(len(result['suppliers']), 2)

    def test_app_routes(self):
        """Test Flask application routes (smoke test)."""
        app.config['TESTING'] = True
        app.config['LOGIN_DISABLED'] = True # Bypass login for testing routes
        with app.test_client() as client:
            # Test index
            response = client.get('/')
            self.assertEqual(response.status_code, 200)
            
            # Test reports
            response = client.get('/reports/spread')
            self.assertEqual(response.status_code, 200)
            
            response = client.get('/reports/markup')
            self.assertEqual(response.status_code, 200)
            
            # Test API search
            response = client.get('/api/search?q=test')
            self.assertEqual(response.status_code, 200)
            data = json.loads(response.data)
            self.assertIn('items', data)

if __name__ == '__main__':
    unittest.main()
