import unittest
from dbacademy.dbbuild.publish.publishing_info_class import PublishingInfo


class MyTestCase(unittest.TestCase):

    def test_load_text(self):
        pi = PublishingInfo(valid_sample)
        self.assertIsNotNone(pi)

        self.assertIsNotNone(pi.releases)
        self.assertEqual(2, len(pi.releases))
        self.assertEqual("https://github.com/databricks-academy/scalable-machine-learning-with-apache-spark/releases", pi.releases.get("english"))
        self.assertEqual("https://github.com/databricks-academy/scalable-machine-learning-with-apache-spark-japanese/releases", pi.releases.get("japanese"))

        self.assertIsNotNone(pi.announcements)
        self.assertEqual(1, len(pi.announcements.email_addresses))
        self.assertEqual("curriculum-announcements@databricks.com", pi.announcements.email_addresses[0])

        self.assertEqual(1, len(pi.announcements.slack_channels))
        self.assertEqual("#curr-announcements", pi.announcements.slack_channels[0].name)
        self.assertEqual("https://databricks.slack.com/archives/C047ZDDBK89", pi.announcements.slack_channels[0].link)

        self.assertEqual(2, len(pi.docs))

        self.assertTrue("english" in pi.docs)
        eng_doc = pi.docs.get("english")
        self.assertEqual("https://drive.google.com/drive/folders/1w5Y_iMapcF1M_u3pt6_xl4NtoQ9tO6e_", eng_doc.published_folder)
        self.assertEqual("https://script.google.com/home/projects/17JUWGbZ44ConZUc8J9kfozX59hVVNNX3W5r6MyH871RtFhLT_Um057ma/edit", eng_doc.publishing_script)
        self.assertEqual(1, len(eng_doc.links))
        self.assertEqual("https://docs.google.com/presentation/d/1f7cjBndkJjefN53lfq46zk7keNv91b2U7MeRMJ6mjfE/edit", eng_doc.links[0])

        self.assertTrue("japanese" in pi.docs)
        jap_doc = pi.docs.get("japanese")
        self.assertEqual("https://drive.google.com/drive/folders/1w5Y_iMapcF1M_u3pt6_xl4NtoQ9tO6e_", jap_doc.published_folder)
        self.assertEqual("https://script.google.com/home/projects/1MuuKrXvX3m7oB8m4SO_-xndiBmU9pcWw1UsJLQqeSxFizUnaAlxXAXmE/edit", jap_doc.publishing_script)
        self.assertEqual(2, len(jap_doc.links))
        self.assertEqual("https://docs.google.com/presentation/d/1KiA6p6N941dqgXoxxGestRr6bZZvjhBqz8auOKkJNjs/edit", jap_doc.links[0])
        self.assertEqual("https://docs.google.com/presentation/d/jdlq34jrlq3j4rlq3jrlq3ijrl3r4jrlqjf4qli4jrfl/edit", jap_doc.links[1])


if __name__ == '__main__':
    unittest.main()

valid_sample = {
    "release_repos": {
        "english": "https://github.com/databricks-academy/scalable-machine-learning-with-apache-spark/releases",
        "japanese": "https://github.com/databricks-academy/scalable-machine-learning-with-apache-spark-japanese/releases"
    },
    "slides": {
        "english": {
            "published_folder": "https://drive.google.com/drive/folders/1w5Y_iMapcF1M_u3pt6_xl4NtoQ9tO6e_",
            "publishing_script": "https://script.google.com/home/projects/17JUWGbZ44ConZUc8J9kfozX59hVVNNX3W5r6MyH871RtFhLT_Um057ma/edit",
            "slide_decks": [
                "https://docs.google.com/presentation/d/1f7cjBndkJjefN53lfq46zk7keNv91b2U7MeRMJ6mjfE/edit"
            ]
        },
        "japanese": {
            "published_folder": "https://drive.google.com/drive/folders/1w5Y_iMapcF1M_u3pt6_xl4NtoQ9tO6e_",
            "publishing_script": "https://script.google.com/home/projects/1MuuKrXvX3m7oB8m4SO_-xndiBmU9pcWw1UsJLQqeSxFizUnaAlxXAXmE/edit",
            "slide_decks": [
                "https://docs.google.com/presentation/d/1KiA6p6N941dqgXoxxGestRr6bZZvjhBqz8auOKkJNjs/edit",
                "https://docs.google.com/presentation/d/jdlq34jrlq3j4rlq3jrlq3ijrl3r4jrlqjf4qli4jrfl/edit"
            ]
        }
    },
    "announcements": {
        "email_addresses": ["curriculum-announcements@databricks.com"],
        "slack_channels": [
            {"name": "#curr-announcements", "link": "https://databricks.slack.com/archives/C047ZDDBK89"}
        ]
    }
}
