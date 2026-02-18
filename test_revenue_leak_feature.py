import os
import pandas as pd
import unittest
from unittest.mock import patch, MagicMock
import scout_agent
import analyst_agent
import sniper_agent

class TestFullSequence(unittest.TestCase):

    def setUp(self):
        self.client_key = "test_client"
        self.leads_file = f"leads_queue_{self.client_key}.csv"
        self.audits_file = f"audits_to_send_{self.client_key}.csv"
        
        # Create a dummy leads file for the analyst agent
        leads_data = {
            "URL": ["http://test.com"],
            "Status": ["Unscanned"]
        }
        pd.DataFrame(leads_data).to_csv(self.leads_file, index=False)

    def tearDown(self):
        # Clean up created files
        for file_path in [self.leads_file, self.audits_file]:
            if os.path.exists(file_path):
                os.remove(file_path)

    @patch('scout_agent.GoogleSearch')
    @patch('analyst_agent.fetch_site_text')
    @patch('analyst_agent.analyze_with_gemini')
    @patch('sniper_agent.smtplib.SMTP')
    @patch('sniper_agent.enrich_email_with_hunter')
    def test_full_sequence(self, mock_hunter, mock_smtp, mock_analyze_gemini, mock_fetch_text, mock_google_search):
        # Mock Scout Agent
        mock_search_instance = mock_google_search.return_value
        mock_search_instance.get_dict.return_value = {
            "organic_results": [
                {"link": "http://test.com"}
            ]
        }

        # Mock Analyst Agent
        mock_fetch_text.return_value = ("<html><body>Test content</body></html>", {"Contact_Page": None})
        mock_analyze_gemini.return_value = "Test Pain Point Summary"

        # Mock Sniper Agent
        mock_hunter.return_value = "test@example.com"

        # Run the full sequence
        scout_agent.scout_leads("test_niche", "test_location", self.client_key)
        analyst_agent.main(self.client_key)
        sniper_agent.main(self.client_key)

        # Assertions
        self.assertTrue(os.path.exists(self.leads_file))
        self.assertTrue(os.path.exists(self.audits_file))

        audits_df = pd.read_csv(self.audits_file)
        self.assertEqual(len(audits_df), 1)
        self.assertEqual(audits_df["URL"][0], "http://test.com")
        self.assertEqual(audits_df["Pain_Point_Summary"][0], "Test Pain Point Summary")
        self.assertEqual(audits_df["Status"][0], "Sent")
        self.assertTrue(audits_df["Audit Attached"][0])

if __name__ == '__main__':
    unittest.main()
