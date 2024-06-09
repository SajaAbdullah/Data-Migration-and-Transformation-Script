from prepare_questions import process_questions
from utils import data_write

ENV = "stage"

questions, question_groups = process_questions(env=ENV)

data_write("QB_question_group.json", question_groups)
data_write("QB_questions.json", questions)
