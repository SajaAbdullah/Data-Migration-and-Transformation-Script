def get_chapter_lps(
    book_mapping, lp_id_map_dict, book_uuid: str, chapter_number: int | str
) -> list[dict]:
    chapter_lps = []
    chapter_mappings = list(
        filter(
            lambda mapping: mapping["book"] == book_uuid
            and mapping["chapter_no"] == str(chapter_number),
            book_mapping,
        )
    )

    if not chapter_mappings:
        return None

    broken_lps = set()
    # make BookChapterLessonPlan fixture
    for mapping in chapter_mappings:
        # lp may not exist
        if mapping["lp_uuid"] in lp_id_map_dict:
            chapter_lps.append(
                {
                    "lesson_plan": lp_id_map_dict[mapping["lp_uuid"]],
                    "lp_index": int(mapping["lp_index"]),
                }
            )
        else:
            broken_lps.add(mapping["lp_uuid"])

    if broken_lps:
        print(f"\nBroken LPs: {list(broken_lps)}")
        print(f"{len(broken_lps)} Broken LPs found.")

    return chapter_lps
