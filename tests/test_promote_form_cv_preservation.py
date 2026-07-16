import unittest

from hubspot_course_sheet_guardrails import (
    COURSE_SHEET_FORM_DISPLAY_HEADER,
    COURSE_SHEET_HEADER,
    COURSE_SHEET_INDEX,
    write_sheet_values,
)
from promote_hubspot_course_staging import (
    apply_preserved_form_cv,
    preserve_form_cv_by_content_id,
)


def row(content_id: str, count="", breakdown="") -> list[str]:
    values = [""] * len(COURSE_SHEET_HEADER)
    values[COURSE_SHEET_INDEX["メール件名（HubSpotリンク）"]] = (
        f'=HYPERLINK("https://app.hubspot.com/email/39827439/details/{content_id}/performance","件名")'
    )
    values[COURSE_SHEET_INDEX["CV数"]] = count
    values[COURSE_SHEET_INDEX["CV内訳"]] = breakdown
    return values


class FakeWorksheet:
    title = "USCPA"

    def __init__(self):
        self.updated = None

    def clear(self):
        return None

    def update(self, values, value_input_option=None):
        self.updated = values
        return None


class PromotionPreservationTests(unittest.TestCase):
    def test_existing_form_values_are_preserved_and_new_email_is_blank(self):
        existing = [COURSE_SHEET_FORM_DISPLAY_HEADER, row("100", "4", "オンライン体験：4")]
        preserved = preserve_form_cv_by_content_id(existing, COURSE_SHEET_INDEX)
        promoted = [row("100", "999", "GA4"), row("200", "12", "GA4")]

        new_count = apply_preserved_form_cv(promoted, preserved, COURSE_SHEET_INDEX)

        self.assertEqual(1, new_count)
        self.assertEqual("4", promoted[0][COURSE_SHEET_INDEX["CV数"]])
        self.assertEqual("オンライン体験：4", promoted[0][COURSE_SHEET_INDEX["CV内訳"]])
        self.assertEqual("", promoted[1][COURSE_SHEET_INDEX["CV数"]])
        self.assertEqual("", promoted[1][COURSE_SHEET_INDEX["CV内訳"]])

    def test_duplicate_existing_content_id_aborts(self):
        existing = [COURSE_SHEET_FORM_DISPLAY_HEADER, row("100"), row("100")]

        with self.assertRaises(SystemExit):
            preserve_form_cv_by_content_id(existing, COURSE_SHEET_INDEX)

    def test_live_writer_uses_form_headers(self):
        worksheet = FakeWorksheet()

        write_sheet_values(
            worksheet,
            [COURSE_SHEET_HEADER, row("100")],
            apply_formatting=False,
            display_header=COURSE_SHEET_FORM_DISPLAY_HEADER,
        )

        self.assertEqual(COURSE_SHEET_FORM_DISPLAY_HEADER, worksheet.updated[0])


if __name__ == "__main__":
    unittest.main()
