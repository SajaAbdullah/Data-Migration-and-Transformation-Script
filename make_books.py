from prepare_books import process_books
from utils import data_write

ENV = "stage"

transformed_books, transformed_chapters, transformed_chapters_lps = process_books(
    env=ENV
)

data_write("BL_books.json", transformed_books)
data_write("BL_book_chapters.json", transformed_chapters)
data_write("BL_book_chapters_lps.json", transformed_chapters_lps)

print("book, book chapters, and book_mapping fixtures done")
