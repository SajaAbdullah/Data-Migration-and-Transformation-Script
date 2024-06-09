from utils import data_load, data_write

lesson_plans_fixture = data_load("final_updated_lessons_v3.json")
prod_lps = 0
ready_for_review = 0

for lesson_plan in lesson_plans_fixture:
    lp_field = lesson_plan["fields"]
    if lp_field["status"] == "OnProd":
        prod_lps = prod_lps + 1
    else:
        ready_for_review = ready_for_review + 1

print("prod_lps", prod_lps)
print("ready_for_review_lps", ready_for_review)
# Step 2: Split the data into two halves
total_objects = len(lesson_plans_fixture)
middle_index = total_objects // 2

first_half = lesson_plans_fixture[:middle_index]
second_half = lesson_plans_fixture[middle_index:]


data_write("LP_lesson_plans_first_half_v2.json", first_half)
data_write("LP_lesson_plans_second_half_v2.json", second_half)
