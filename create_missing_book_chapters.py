"""
This scripts searches the book chapters in MongoDB of the books in BL_books.json that don't have any chapter
in BL_book_chapters.json. And creates the fixtures for the missing book chapters and book chapter lps.
"""

import uuid
from collections import defaultdict

from db_service import get_collection
from prepare_books import (
    BOOK_CHAPTER_MODEL_REF,
    BookChapter_LessonPlan_MODEL_REF,
    extract_chapter_fields,
    merge_v1_v2_mappings,
)
from utils import data_load, data_write

ENV = "stage"


book_chapter_pk_count = 14059
book_chapter_lp_pk_count = 89079


def create_missing_book_chapters():
    new_book_chapters = []
    new_book_chapter_lps = []

    book_with_missing_chapters = get_books_with_no_chapters()

    print("Books with missing chapters: ", len(book_with_missing_chapters))

    if book_with_missing_chapters:
        book_uuids = [book["fields"]["uuid"] for book in book_with_missing_chapters]

        book_chapters_col = get_collection(
            database_name="book-library", col_name="chapters", client=ENV
        )
        chapters = list(
            book_chapters_col.find({"book": {"$in": book_uuids}}, {"_id": 0})
        )
        book_chapters = defaultdict(list)
        for chapter in chapters:
            book_chapters[chapter["book"]].append(chapter)

        print("Fetching and merging the v1 v2 mappings")
        merged_mappings = merge_v1_v2_mappings(book_uuids=book_uuids, env=ENV)
        book_chapter_lp_mappings = defaultdict(list)
        for mapping in merged_mappings:
            book_chapter_lp_mappings[mapping["book"]].append(mapping)

        lp_uuid_id_map = {
            lp["fields"]["uuid"]: lp["pk"]
            for lp in data_load("LP_lesson_plans_first_half.json")
            + data_load("LP_lesson_plans_second_half.json")
        }

        for book in book_with_missing_chapters:
            single_book_chapters, single_book_chapter_lps = get_book_chapters(
                book,
                book_chapters.get(book["fields"]["uuid"], []),
                book_chapter_lp_mappings.get(book["fields"]["uuid"], []),
                lp_uuid_id_map,
            )
            new_book_chapters.extend(single_book_chapters)
            new_book_chapter_lps.extend(single_book_chapter_lps)

            print(
                f"Book -> {book['fields']['uuid']} | Chapters -> {len(single_book_chapters)} | Lps -> {len(single_book_chapter_lps)}"
            )

        data_write("BL_missing_book_chapters.json", new_book_chapters)
        data_write("BL_missing_book_chapter_lps.json", new_book_chapter_lps)


def get_books_with_no_chapters():
    books = data_load("BL_books.json")

    chapters = data_load("BL_book_chapters.json")

    book_chapters = defaultdict(list)
    for chapter in chapters:
        book_chapters[chapter["fields"]["book"]].append(chapter["pk"])

    books_with_missing_chapters = []
    for book in books:
        if book["pk"] not in book_chapters:
            books_with_missing_chapters.append(book)

    return books_with_missing_chapters


def get_book_chapters(
    book: dict,
    book_chapters_mongo: list[dict],
    book_chapter_lp_mappings_mongo: list[dict],
    lp_uuid_id_map: dict,
):
    book_chapters = []
    book_chapter_lps = []
    global book_chapter_pk_count
    global book_chapter_lp_pk_count

    chapters_data = []
    if book_chapter_lp_mappings_mongo:
        # make book chapters set from book mappings
        mapping_book_chapters = {}
        for mapping in book_chapter_lp_mappings_mongo:
            if mapping["chapter_no"] not in mapping_book_chapters:
                mapping_book_chapters[mapping["chapter_no"]] = mapping["chapter"]

        book_chapters_in_chapters_col_dict = {
            chapter["chapter_no"]: chapter for chapter in book_chapters_mongo
        }

        for chapter_no, chapter_name in mapping_book_chapters.items():
            if chapter_no in book_chapters_in_chapters_col_dict:
                chapters_data.append(book_chapters_in_chapters_col_dict[chapter_no])
            else:
                chapters_data.append(
                    {
                        "uuid": str(uuid.uuid4()),
                        "name": chapter_name,
                        "chapter_no": chapter_no,
                    }
                )

    elif book_chapters_mongo:
        for chapter in book_chapters_mongo:
            chapters_data.append(chapter)

    # create book chapter lp relations
    for chapter in chapters_data:
        book_chapter_pk_count += 1
        chapter_fields = extract_chapter_fields(
            chapter=chapter, book_status=book["fields"]["status"]
        )
        chapter_fields["book"] = book["pk"]

        book_chapters.append(
            {
                "model": BOOK_CHAPTER_MODEL_REF,
                "pk": book_chapter_pk_count,
                "fields": chapter_fields,
            }
        )

        chapter_lps = get_chapter_lps(
            book_chapter_lp_mappings_mongo=book_chapter_lp_mappings_mongo,
            book_chapter=chapter,
            lp_uuid_id_map=lp_uuid_id_map,
        )

        for chapter_lp in chapter_lps:
            book_chapter_lp_pk_count += 1
            book_chapter_lps.append(
                {
                    "model": BookChapter_LessonPlan_MODEL_REF,
                    "pk": book_chapter_lp_pk_count,
                    "fields": {
                        "book_chapter": book_chapter_pk_count,
                        "lesson_plan": chapter_lp["lesson_plan"],
                        "lp_index": chapter_lp["lp_index"],
                    },
                }
            )

    return book_chapters, book_chapter_lps


def get_chapter_lps(
    book_chapter_lp_mappings_mongo: list[dict],
    book_chapter: dict,
    lp_uuid_id_map: dict,
) -> list[dict]:
    chapter_lps = []

    chapter_lp_uuids = []
    broken_lps = []
    for mapping in book_chapter_lp_mappings_mongo:
        if mapping["chapter_no"] == str(book_chapter["chapter_no"]):
            if mapping["lp_uuid"] in lp_uuid_id_map:
                # if mapping is already present(LP is repeating in the chapter) then skip.
                # LP should not be repeated in the chapter
                if mapping["lp_uuid"] not in chapter_lp_uuids:
                    chapter_lp_uuids.append(mapping["lp_uuid"])
                    chapter_lps.append(
                        {
                            "lesson_plan": lp_uuid_id_map[mapping["lp_uuid"]],
                            "lp_index": int(mapping["lp_index"]),
                        }
                    )
            else:
                broken_lps.append(mapping["lp_uuid"])

    if broken_lps:
        print(f"\nBroken LPs: {list(broken_lps)}")
        print(f"Broken LPs Count: {len(broken_lps)}")

    return chapter_lps


create_missing_book_chapters()
