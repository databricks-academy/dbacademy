import unittest
from dbacademy.dbbuild.publish.publishing_info_class import PublishingInfo


class MyTestCase(unittest.TestCase):

    def test_load_text(self):
        pi = PublishingInfo(valid_sample)
        self.assertIsNotNone(pi)

        self.assertIsNotNone(pi.announcements)
        self.assertEqual(1, len(pi.announcements.email_addresses))
        self.assertEqual("curriculum-announcements@databricks.com", pi.announcements.email_addresses[0])

        self.assertEqual(1, len(pi.announcements.slack_channels))
        self.assertEqual("#curr-announcements", pi.announcements.slack_channels[0].name)
        self.assertEqual("https://databricks.slack.com/archives/C047ZDDBK89", pi.announcements.slack_channels[0].url)

        self.assertEqual(2, len(pi.translations))

        self.assertTrue("english", pi.translations)
        trans = pi.translations.get("english")
        self.assertIsNotNone(trans)
        self.assertEqual("english", trans.language)
        self.assertEqual("https://github.com/databricks-academy/scalable-machine-learning-with-apache-spark-english/releases", trans.release_repo)
        self.assertEqual("https://drive.google.com/drive/folders/1w5Y_iMapcF1M_u3pt6_xl4NtoQ9tO6e_", trans.published_docs_folder)
        self.assertEqual("https://script.google.com/home/projects/17JUWGbZ44ConZUc8J9kfozX59hVVNNX3W5r6MyH871RtFhLT_Um057ma/edit", trans.publishing_script)
        self.assertEqual(1, len(trans.document_links))
        self.assertEqual("https://docs.google.com/presentation/d/1f7cjBndkJjefN53lfq46zk7keNv91b2U7MeRMJ6mjfE/edit", trans.document_links[0])

        self.assertTrue("japanese", pi.translations)
        trans = pi.translations.get("japanese")
        self.assertIsNotNone(trans)
        self.assertEqual("japanese", trans.language)
        self.assertEqual("https://github.com/databricks-academy/scalable-machine-learning-with-apache-spark-japanese/releases", trans.release_repo)
        self.assertEqual("https://drive.google.com/drive/folders/1w5Y_iMapcF1M_u3pt6_xl4NtoQ9tO6e_", trans.published_docs_folder)
        self.assertEqual("https://script.google.com/home/projects/1MuuKrXvX3m7oB8m4SO_-xndiBmU9pcWw1UsJLQqeSxFizUnaAlxXAXmE/edit", trans.publishing_script)
        self.assertEqual(2, len(trans.document_links))
        self.assertEqual("https://docs.google.com/presentation/d/1KiA6p6N941dqgXoxxGestRr6bZZvjhBqz8auOKkJNjs/edit", trans.document_links[0])
        self.assertEqual("https://docs.google.com/presentation/d/jdlq34jrlq3j4rlq3jrlq3ijrl3r4jrlqjf4qli4jrfl/edit", trans.document_links[1])


if __name__ == '__main__':
    unittest.main()

valid_sample = {
    "announcements": {
        "email_addresses": ["curriculum-announcements@databricks.com"],
        "slack_channels": [
            {"name": "#curr-announcements", "url": "https://databricks.slack.com/archives/C047ZDDBK89"}
        ]
    },
    "translations": {
        "english": {
            "release_repo": "https://github.com/databricks-academy/scalable-machine-learning-with-apache-spark-english/releases",
            "published_docs_folder": "https://drive.google.com/drive/folders/1w5Y_iMapcF1M_u3pt6_xl4NtoQ9tO6e_",
            "publishing_script": "https://script.google.com/home/projects/17JUWGbZ44ConZUc8J9kfozX59hVVNNX3W5r6MyH871RtFhLT_Um057ma/edit",
            "document_links": [
                "https://docs.google.com/presentation/d/1f7cjBndkJjefN53lfq46zk7keNv91b2U7MeRMJ6mjfE/edit"
            ]
        },
        "japanese": {
            "release_repo": "https://github.com/databricks-academy/scalable-machine-learning-with-apache-spark-japanese/releases",
            "published_docs_folder": "https://drive.google.com/drive/folders/1w5Y_iMapcF1M_u3pt6_xl4NtoQ9tO6e_",
            "publishing_script": "https://script.google.com/home/projects/1MuuKrXvX3m7oB8m4SO_-xndiBmU9pcWw1UsJLQqeSxFizUnaAlxXAXmE/edit",
            "document_links": [
                "https://docs.google.com/presentation/d/1KiA6p6N941dqgXoxxGestRr6bZZvjhBqz8auOKkJNjs/edit",
                "https://docs.google.com/presentation/d/jdlq34jrlq3j4rlq3jrlq3ijrl3r4jrlqjf4qli4jrfl/edit"
            ]
        }
    }
}
