import re
import uuid
from collections import defaultdict

from book_related_lps import get_chapter_lps
from constants import Status
from db_service import get_collection
from utils import data_load, get_is_active

# define reference model
BOOK_MODEL_REF = "book_library.Book"
BOOK_CHAPTER_MODEL_REF = "book_library.BookChapter"
BookChapter_LessonPlan_MODEL_REF = "book_library.BookChapterLessonPlan"

# get lp_id mapping
lp_id_map_dict = {}
lps = data_load("LP_lesson_plans.json")
for lp in lps:
    lp_id_map_dict[lp["fields"]["uuid"]] = lp["pk"]

# get grade subject mapping
grade_subject_id_maps_dict = {}
grade_subjects = data_load("GS_grade_subject.json")
for grade_subject in grade_subjects:
    grade_subject_id_maps_dict[grade_subject["fields"]["uuid"]] = grade_subject["pk"]


def process_books(
    book_uuids=None,
    skip_chapters_and_mappings=False,
    use_existing_pks=False,
    env="stage",
):
    books_col = get_collection(
        database_name="book-library", col_name="books", client=env
    )
    book_chapters_col = get_collection(
        database_name="book-library", col_name="chapters", client=env
    )

    books_filter = {}
    if book_uuids:
        books_filter = {"uuid": {"$in": book_uuids}}

    if use_existing_pks:
        book_uuid_id_map = {
            book["fields"]["uuid"]: book["pk"] for book in data_load("BL_books.json")
        }

    # get data from mongo
    book_docs = list(books_col.find(books_filter, {"_id": 0}))

    if not skip_chapters_and_mappings:
        chapters = list(book_chapters_col.find({}, {"_id": 0}))
        snc_book_mapping_v1_v2_merged = merge_v1_v2_mappings(env=env)

        print("all data fetched")

        # group each book mappings into {book_uuid: [mapping1, mapping2, ...]}
        book_mappings_v3 = defaultdict(list)
        # convert lp_slo_mapping to dict in form of {slo_uuid: [lp_uuid1, lp_uuid2, ...]}
        for mapping in snc_book_mapping_v1_v2_merged:
            book_uuid = mapping["book"]
            book_mappings_v3[book_uuid].append(mapping)

        print(f"total number of books in book_mappings_v3 {len(book_mappings_v3)}")

        # group each book chapter into {book_uuid: [chapter1, chapter2, ...]}
        book_chapters = defaultdict(list)
        # convert lp_slo_mapping to dict in form of {slo_uuid: [lp_uuid1, lp_uuid2, ...]}
        for chapter in chapters:
            book_uuid = chapter["book"]
            book_chapters[book_uuid].append(chapter)

        print(f"total number of books in chapter collection {len(book_chapters)}")

    transformed_books = []
    book_primary_key = 0

    transformed_chapters = []
    chapter_primary_key = 0

    transformed_chapters_lps = []
    chapter_lps_primary_key = 0

    for book in book_docs:
        if use_existing_pks:
            book_primary_key = book_uuid_id_map[book["uuid"]]
        else:
            book_primary_key = book_primary_key + 1
        book_fields = extract_book_fields(book, env)

        # map foreign keys
        book_fields["grade_subject"] = grade_subject_id_maps_dict[book["subject"]]

        transformed_books.append(
            {"model": BOOK_MODEL_REF, "pk": book_primary_key, "fields": book_fields}
        )

        if skip_chapters_and_mappings:
            continue

        # create book chapters fixtures
        final_book_chapters = []
        final_book_mappings = []

        if book["uuid"] in book_mappings_v3:
            final_book_mappings = book_mappings_v3[book["uuid"]]
            # make book chapters set from book mappings
            mapping_book_chapters = {}
            for mapping in final_book_mappings:
                if mapping["chapter_no"] not in mapping_book_chapters:
                    mapping_book_chapters[mapping["chapter_no"]] = mapping["chapter"]

            book_chapters_in_chapters_col_dict = {
                chapter["chapter_no"]: chapter
                for chapter in book_chapters.get(book["uuid"], [])
            }

            for chapter_no, chapter_name in mapping_book_chapters.items():
                if chapter_no in book_chapters_in_chapters_col_dict:
                    final_book_chapters.append(
                        book_chapters_in_chapters_col_dict[chapter_no]
                    )
                else:
                    final_book_chapters.append(
                        {
                            "uuid": str(uuid.uuid4()),
                            "name": chapter_name,
                            "chapter_no": chapter_no,
                        }
                    )
        else:
            # if book mapping not found but book chapters exist
            if book["uuid"] in book_chapters:
                final_book_chapters = book_chapters[book["uuid"]]
            else:
                # if there is no data in chapters and mappings then skip creating book chapters
                continue

        for chapter in final_book_chapters:
            chapter_primary_key = chapter_primary_key + 1
            chapter_fields = extract_chapter_fields(
                chapter=chapter, book_status=book_fields["status"]
            )
            chapter_fields["book"] = book_primary_key

            transformed_chapters.append(
                {
                    "model": BOOK_CHAPTER_MODEL_REF,
                    "pk": chapter_primary_key,
                    "fields": chapter_fields,
                }
            )

            # create book chapter lesson plan many to many relation fixture
            chapter_lps = get_chapter_lps(
                book_mapping=final_book_mappings,
                lp_id_map_dict=lp_id_map_dict,
                book_uuid=book["uuid"],
                chapter_number=chapter["chapter_no"],
            )
            if chapter_lps:
                book_chapter_lps = []
                for chapter_lp in chapter_lps:
                    # check if mapping already exit
                    book_chapter = f"{chapter_primary_key}-{chapter_lp['lesson_plan']}"

                    # if mapping is already present(LP is repeating in the chapter) then skip
                    # LP should not be repeated in the chapter
                    if book_chapter in book_chapter_lps:
                        continue
                    book_chapter_lps.append(book_chapter)

                    chapter_lps_primary_key = chapter_lps_primary_key + 1

                    transformed_chapters_lps.append(
                        {
                            "model": BookChapter_LessonPlan_MODEL_REF,
                            "pk": chapter_lps_primary_key,
                            "fields": {
                                "book_chapter": chapter_primary_key,
                                "lesson_plan": chapter_lp["lesson_plan"],
                                "lp_index": chapter_lp["lp_index"],
                            },
                        }
                    )

    return transformed_books, transformed_chapters, transformed_chapters_lps


def merge_v1_v2_mappings(book_uuids: list[str] = None, env: str = "stage"):
    books_filter = {}
    if book_uuids:
        books_filter["book"] = {"$in": book_uuids}

    snc_book_mapping_col = get_collection(
        database_name="book-library", col_name="snc-book-mapping", client=env
    )
    snc_book_mapping_v2_col = get_collection(
        database_name="book-library", col_name="snc-book-mapping-v2", client=env
    )
    lp_slo_mapping_col = get_collection(
        database_name="lp-library", col_name="lp-slo-mapping", client=env
    )
    books_col = get_collection(
        database_name="book-library", col_name="books", client=env
    )

    snc_book_mapping = list(snc_book_mapping_col.find(books_filter, {"_id": 0}))
    snc_book_mapping_v2 = list(snc_book_mapping_v2_col.find(books_filter, {"_id": 0}))

    slo_uuids = [mapping["slo_uuid"] for mapping in snc_book_mapping]
    lp_slo_mapping = list(lp_slo_mapping_col.find({"slo_uuid": {"$in": slo_uuids}}, {"_id": 0}))
    slo_lessons = defaultdict(list)
    # convert lp_slo_mapping to dict in form of {slo_uuid: [lp_uuid1, lp_uuid2, ...]}
    for mapping in lp_slo_mapping:
        slo_uuid = mapping["slo_uuid"]
        lp_uuid = mapping["lp_uuid"]
        slo_lessons[slo_uuid].append(lp_uuid)

    # group the mappings in book->chapter->mapppings
    grouped_book_mappings = {}
    for mapping in snc_book_mapping:
        if mapping["book"] not in grouped_book_mappings:
            grouped_book_mappings[mapping["book"]] = {}
        if mapping["chapter_no"] not in grouped_book_mappings[mapping["book"]]:
            grouped_book_mappings[mapping["book"]][mapping["chapter_no"]] = []

        grouped_book_mappings[mapping["book"]][mapping["chapter_no"]].append(mapping)

    transformed_snc_book_mapping = []
    for book_uuid, chapters in grouped_book_mappings.items():
        for chapter_no, mappings in chapters.items():
            try:
                sorted_mappings = sorted(mappings, key=lambda x: int(x["slo_index"]))
            except:
                print(f"Book uuid -> {book_uuid} | Chapter No -> {chapter_no}")
                raise

            lp_index = 1
            for single_mapping in sorted_mappings:
                slo_lps = slo_lessons[single_mapping["slo_uuid"]]
                for lp_uuid in slo_lps:
                    new_mapping = single_mapping.copy()
                    new_mapping["lp_uuid"] = lp_uuid
                    new_mapping["lp_index"] = lp_index
                    transformed_snc_book_mapping.append(new_mapping)
                    lp_index += 1

    # merge the mappings
    merged_mappings = {}

    # snc_book_mapping_v2 is given priority because it has the latest data
    for mapping in snc_book_mapping_v2:
        if (
            mapping["book"],
            mapping["chapter_no"],
            mapping["lp_uuid"],
        ) not in merged_mappings:
            merged_mappings[
                (mapping["book"], mapping["chapter_no"], mapping["lp_uuid"])
            ] = mapping

    selected_books_for_lp_experiment = books_col.find(
        {"selected_for_lp_experiment": True}, {"_id": 0, "uuid": 1}
    ).distinct("uuid")

    for mapping in transformed_snc_book_mapping:
        # if the book is selected for lp experiment, then only use the snc_book_mapping_v2 data
        # because the mappings of these books are recently updated in snc_book_mapping_v2
        # from the new book mapping tool (restructuring tool)
        if mapping["book"] not in selected_books_for_lp_experiment:
            if (
                mapping["book"],
                mapping["chapter_no"],
                mapping["lp_uuid"],
            ) not in merged_mappings:
                merged_mappings[
                    (mapping["book"], mapping["chapter_no"], mapping["lp_uuid"])
                ] = mapping

    # group the mappings again and adjust the lp_index
    grouped_merged_mappings = {}
    for mapping in merged_mappings.values():
        if mapping["book"] not in grouped_merged_mappings:
            grouped_merged_mappings[mapping["book"]] = {}
        if mapping["chapter_no"] not in grouped_merged_mappings[mapping["book"]]:
            grouped_merged_mappings[mapping["book"]][mapping["chapter_no"]] = []

        grouped_merged_mappings[mapping["book"]][mapping["chapter_no"]].append(mapping)

    final_mappings = []
    for book_uuid, chapters in grouped_merged_mappings.items():
        for chapter_no, mappings in chapters.items():
            sorted_mappings = sorted(mappings, key=lambda x: int(x["lp_index"]))
            lp_index = 1
            for single_mapping in sorted_mappings:
                single_mapping["lp_index"] = lp_index
                lp_index += 1
                final_mappings.append(single_mapping)

    return final_mappings


# make book fixture
def extract_book_fields(book: dict, env) -> dict:
    """transform book main fields"""
    if env == "prod":
        book_status = Status.ON_PROD.value
    else:
        book_status = get_book_status(data_dict=book, key="uploaded_to_prod")
    return {
        "title": book["name"],
        "publisher": book["publisher"],
        "published_year": transform_published_year_to_int(
            data_dict=book, key="published_year"
        ),
        "total_chapters": get_key_value(data_dict=book, key="total_chapters"),
        "edition": get_key_value(data_dict=book, key="edition"),
        "pdf_url": get_key_value(data_dict=book, key="book_link"),
        "series": get_key_value(data_dict=book, key="series"),
        "buffer_pages": 0
        if ("buffer_pages" not in book or book["buffer_pages"] in ("", None))
        else book["buffer_pages"],
        "cover_image": book["cover_page"],
        "book_text": get_key_value(data_dict=book, key="book_text"),
        "uuid": book["uuid"],
        "grade_subject": book["subject"],
        "status": book_status,
        "is_active": get_is_active(data_dict=book, status=book_status),
    }


def extract_chapter_fields(chapter, book_status) -> dict:
    """transform book chapter main fields"""
    return {
        "title": chapter["name"],
        "chapter_number": int(chapter["chapter_no"]),
        "chapter_text": get_key_value(data_dict=chapter, key="chapter_text"),
        "start_page": get_key_value(data_dict=chapter, key="chapter_start_page"),
        "end_page": get_key_value(data_dict=chapter, key="chapter_end_page"),
        "uuid": chapter["uuid"],
        "status": book_status,
        "is_active": get_is_active(data_dict=chapter, status=book_status),
    }


def transform_published_year_to_int(data_dict, key):
    value = ""
    if key not in data_dict or data_dict[key] in ("", None):
        return None
    else:
        value = data_dict[key]
        # Use regular expression to find and extract the digits
        match = re.search(r"\d+", value)
        if match:
            # Extract the matched digits and convert to an integer
            return int(match.group())
        else:
            return int(value)


def get_key_value(data_dict, key):
    if key not in data_dict or data_dict[key] in ("", None):
        return None
    else:
        return data_dict[key]


def get_book_status(data_dict, key):
    if data_dict.get(key, True):
        return Status.ON_PROD.value
    else:
        return Status.READY_FOR_REVIEW.value
