from collections import defaultdict

from langdetect import detect

from constants import Status
from db_service import get_collection
from utils import data_load, get_is_active

# define reference model
QUESTION_MODEL_REF = "question_bank.Question"
QUESTION_GROUP_MODEL_REF = "question_bank.QuestionGroup"

# load lessons fixture and create dict of (lp-uuid: (lp-id, grade-subject-id))

lessons = data_load("LP_lesson_plans.json")
lp_id_grade_subject_mapping = {}

for data in lessons:
    uuid = data["fields"]["uuid"]
    pk = data["pk"]
    grade_subject = data["fields"]["grade_subject"]
    lp_id_grade_subject_mapping[uuid] = {"pk": pk, "grade_subject": grade_subject}


lmsreborn_core_question_types_map = {
    "mcq": "multiple-choice",
    "ftb": "fill-the-blanks",
    "saq": "short-answer",
    "laq": "long-answer",
    "reading&writing": "reading-writing",
    "speaking&listening": "speaking-listening",
    "creativewriting": "creative-writing",
}


def process_questions(
    question_uuids: list = None,
    use_existing_question_groups: bool = False,
    use_existing_pks: bool = False,
    env: str = "stage",
):
    # merge the questions from questions and questions-v2 collection
    merged_questions = merge_v1_v2_questions(question_uuids=question_uuids, env=env)

    if use_existing_pks:
        question_uuid_id_map = {
            question["fields"]["uuid"]: question["pk"]
            for question in data_load("QB_questions.json")
        }

    missing_lps = set()
    questions_with_no_lp_slo = set()

    questions = []

    question_groups = []
    question_group_pk_count = 1

    # comprehension passage question group id map
    if use_existing_question_groups:
        question_group_ids = {
            question_group["fields"]["comprehension_passage"]: question_group["pk"]
            for question_group in data_load("QB_question_group.json")
        }
    else:
        question_group_ids = {}

    for index, question in enumerate(merged_questions, start=1):
        question_fields = extract_question_fields(
            question, missing_lps, questions_with_no_lp_slo
        )

        # group the comprehension (reading&writing) questions in QuestionGroup model
        if question.get("comprehensionpassage"):
            # check if comprehension passage question group already exist
            # if not create new one
            if question["comprehensionpassage"] not in question_group_ids:
                if use_existing_question_groups:
                    raise Exception(
                        f"Comprehension passage question group with comprehension passage: {question['comprehensionpassage']} not found"
                    )
                question_groups.append(
                    {
                        "model": QUESTION_GROUP_MODEL_REF,
                        "pk": question_group_pk_count,
                        "fields": {
                            "comprehension_passage": question["comprehensionpassage"]
                        },
                    }
                )
                question_group_ids[
                    question["comprehensionpassage"]
                ] = question_group_pk_count
                question_group_id = question_group_pk_count
                question_group_pk_count += 1
            else:
                question_group_id = question_group_ids[question["comprehensionpassage"]]

            question_fields["group"] = question_group_id

        else:
            question_fields["group"] = None

        questions.append(
            {
                "model": QUESTION_MODEL_REF,
                "pk": index
                if not use_existing_pks
                else question_uuid_id_map[question["uuid"]],
                "fields": question_fields,
            }
        )

    if missing_lps:
        print(f"Missing LP(s): {list(missing_lps)}")
        print(f"{len(missing_lps)} Missing LP(s) found in questions.")

    if questions_with_no_lp_slo:
        print(f"\nQuestions with no LP/SLO: {list(questions_with_no_lp_slo)}")
        print(f"{len(questions_with_no_lp_slo)} Questions found with no LP/SLO.")

    return questions, question_groups


def merge_v1_v2_questions(question_uuids: list = None, env: str = "stage"):
    """
    Merges the questions from questions and questions-v2 collection
    and writes the merged data to questions_v1_v2_merged.json(local file)

    questions-v2 collection contains lp_uuid and slo_uuid in lp_slos list
    while questions collection contains only slo_uuid

    questions-v2 is given priority over questions collection
    i.e. if a question is present in both collections, the question from questions-v2 will be used
    The question content is same across both collections, the only difference is the lp_slos list
    that contains the lp_uuids and slo_uuids
    """

    question_filter = {}
    if question_uuids:
        question_filter = {"uuid": {"$in": question_uuids}}

    questions = list(
        get_collection(
            database_name="question-bank", col_name="questions", client=env
        ).find(question_filter, {"_id": 0})
    )
    questions_v2 = list(
        get_collection(
            database_name="question-bank", col_name="questions-v2", client=env
        ).find(question_filter, {"_id": 0})
    )

    lp_slo_mapping = list(
        get_collection(
            database_name="lp-library", col_name="lp-slo-mapping", client=env
        ).find({}, {"_id": 0})
    )

    slo_lessons = defaultdict(list)
    # convert lp_slo_mapping to dict in form of {slo_uuid: [lp_uuid1, lp_uuid2, ...]}
    for mapping in lp_slo_mapping:
        slo_uuid = mapping["slo_uuid"]
        lp_uuid = mapping["lp_uuid"]
        slo_lessons[slo_uuid].append(lp_uuid)
    print("all data fetched")

    questions_v2_uuids = [question["uuid"] for question in questions_v2]

    merged_questions = []

    # slos that don't exist or don't have any mappings in lp-slo-mapping
    broken_slos = set()

    # Add lp_slos(lp_uuid, slo_uuid) in question that are not present in questions_v2
    # and add to merged_questions
    for question in questions:
        if question["uuid"] not in questions_v2_uuids:
            slo_lps = slo_lessons.get(question["slo_uuid"], [])
            if not slo_lps:
                broken_slos.add(question["slo_uuid"])
            question["lp_slos"] = [{"lp_uuid": lp_uuid} for lp_uuid in slo_lps]
            merged_questions.append(question)

    print("transformation done")

    # Add questions_v2 to merged_questions
    merged_questions.extend(questions_v2)

    if broken_slos:
        print(f"Broken SLO(s): {list(broken_slos)}")
        print(
            f"{len(broken_slos)} Broken SLO(s) found, These SLOs don't exist or don't have any mappings in lp-slo-mapping"
        )

    return merged_questions


def extract_question_fields(
    question: dict, missing_lps, questions_with_no_lp_slo
) -> dict:
    return {
        "question_statement": question["statement"] if question["statement"] else None,
        "question_image": question["image"] if question["image"] else None,
        "question_format": question["question_type"],
        "uuid": question["uuid"],
        "grade_subject": get_question_grade_subject(question, questions_with_no_lp_slo),
        "score": get_key_value(data_dict=question, key="marks"),
        "marking_scheme": get_key_value(data_dict=question, key="markingscheme"),
        "type": lmsreborn_core_question_types_map.get(question["type"]),
        "assessment_type": question["assessment_type"],
        "supported_languages": identify_language(question["statement"]),
        "lesson_plans": get_question_lp_ids(question, missing_lps),
        "question_tags": get_key_value(data_dict=question, key="question_tags"),
        "author": get_key_value(data_dict=question, key="author"),
        "answer_option": get_answer_option(question=question),
        "question_status": question.get("question_status", Status.ON_PROD.value),
        "is_active": get_is_active(
            data_dict=question, status=question.get("question_status", Status.ON_PROD.value)
        ),
    }


def get_answer_option(question):
    ans_options = []

    if question["type"] == "mcq":
        for option in question["mcq"]:
            if (
                option["statement"] and option["statement"] == question["ans_statement"]
            ) or (option["image"] and option["image"] == question["ans_image"]):
                is_correct = True
            else:
                is_correct = False

            ans_options.append(
                {
                    "statement": option["statement"] if option["statement"] else None,
                    "image": option["image"] if option["image"] else None,
                    "answer_format": question["ans_type"],
                    "is_correct": is_correct,
                }
            )
    elif question["type"] == "ftb":
        ans_options.append(
            {
                "statement": ",".join(question["ans_statement"]),
                "image": None,
                "answer_format": "statement",
                "is_correct": True,
            }
        )
    else:
        ans_options.append(
            {
                "statement": question["ans_statement"]
                if question["ans_statement"]
                else None,
                "image": question["ans_image"] if question["ans_image"] else None,
                "answer_format": question["ans_type"],
                "is_correct": True,
            }
        )

    return ans_options


def identify_language(text):
    try:
        language = detect(text)
        if language == "en":
            return ["english"]
        elif language == "ur":
            return ["urdu"]
        else:
            return None
    except Exception as e:
        return None


def get_key_value(data_dict, key):
    """if key exist return key value"""
    if key not in data_dict or data_dict[key] in ("", None):
        return None
    else:
        return data_dict[key]


def get_question_lp_ids(question, missing_lps) -> list:
    """
    Get the ids of LPs linked with the question
    """

    lp_ids = set()
    for lp_slo in question["lp_slos"]:
        if lp_id := lp_id_grade_subject_mapping.get(lp_slo["lp_uuid"], {}).get("pk"):
            lp_ids.add(lp_id)
        else:
            missing_lps.add(lp_slo["lp_uuid"])

    return list(lp_ids)


def get_question_grade_subject(question, questions_with_no_lp_slo) -> list:
    lp_slos = question.get("lp_slos")
    if lp_slos:
        # get grade_subject from the first lp in the list
        # there can be lps with different grade_subjects linked with the question
        # but considering only the first one as for most cases all the lps linked with a question
        # will have the same grade_subject
        lp_uuid = lp_slos[0]["lp_uuid"]
        return lp_id_grade_subject_mapping.get(lp_uuid, {}).get("grade_subject")
    else:
        questions_with_no_lp_slo.add(question["uuid"])

    return None
