gs:
	python3 make_grade_subject.py
	@echo "Grades and Subjects data complete!"

am:
	python3 make_video_assets.py
	@echo "Video Assets complete!"

slo:
	python3 make_slos.py
	@echo "SLOs data complete!"

sb:
	python3 make_story_books.py
	@echo "Story Books data complete!"

lp:
	python3 make_lps.py
	@echo "LPs data complete!"

qb:
	python3 make_questions.py
	@echo "Question Bank data complete!"

bl:
	python3 make_books.py
	@echo "Book Library data complete!"

tt:
	python3 make_teacher_training.py
	@echo "Teacher training data complete!"

prod:
	python3 make_prod_specific_fixtures.py
	@echo "Production specific data complete!"

all: gs am slo sb lp qb bl tt prod
	@echo "All data complete!"
