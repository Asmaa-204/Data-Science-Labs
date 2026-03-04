import sqlite3
import pandas as pd

conn = sqlite3.connect("library.db")

# ─────────────────────────────────────────────
# Task 1.1
# ─────────────────────────────────────────────
query1_1 = """
SELECT
    b.title,
    a.name as author_name,
    b.publication_year
FROM books b
INNER JOIN authors a ON a.author_id = b.author_id
WHERE b.publication_year > 1960 AND b.genre = 'Fiction'
ORDER BY b.publication_year
"""
pd.read_sql_query(query1_1, conn).to_csv("task1_1.csv", index=False)

# ─────────────────────────────────────────────
# Task 1.2
# ─────────────────────────────────────────────
query1_2 = """
SELECT 
    m.name as member_name,
    m.email,
    COUNT(*) as total_borrowings
FROM borrowings b
INNER JOIN members m ON b.member_id = m.member_id
WHERE membership_type = 'student'
GROUP BY b.member_id
ORDER BY total_borrowings DESC
"""
pd.read_sql_query(query1_2, conn).to_csv("task1_2.csv", index=False)

# ─────────────────────────────────────────────
# Task 1.3
# ─────────────────────────────────────────────
query1_3 = """
SELECT 
    m.membership_type,
    COUNT(DISTINCT m.member_id) AS total_members,
    SUM(b.fine_amount) AS total_fines,
    ROUND(
        SUM(b.fine_amount) / COUNT(DISTINCT m.member_id),
        2
    ) AS avg_fine_per_member
FROM members m
INNER JOIN borrowings b
    ON m.member_id = b.member_id
WHERE b.fine_amount > 0
GROUP BY m.membership_type
"""
pd.read_sql_query(query1_3, conn).to_csv("task1_3.csv", index=False)

# ─────────────────────────────────────────────
# Task 2.1
# ─────────────────────────────────────────────
query2_1 = """
SELECT 
    b.title,
    a.name AS author_name,
    CASE 
        WHEN COUNT(br.borrow_id) = 0 THEN 'Never Borrowed'
        ELSE COUNT(br.borrow_id)
    END AS times_borrowed,
    b.copies_available AS currently_available
FROM books b
INNER JOIN authors a ON a.author_id = b.author_id
LEFT JOIN borrowings br ON br.book_id = b.book_id
GROUP BY b.book_id
ORDER BY COUNT(br.borrow_id) DESC
LIMIT 3
"""
pd.read_sql_query(query2_1, conn).to_csv("task2_1.csv", index=False)

# ─────────────────────────────────────────────
# Task 2.2
# ─────────────────────────────────────────────
query2_2 = """
SELECT
    b.title AS book_title,
    m.name AS borrower_name,
    br.borrow_date,
    br.due_date,
    CAST(
        JULIANDAY(CURRENT_DATE) - JULIANDAY(br.due_date)
        AS INTEGER
    ) AS days_overdue,
    CAST(
        (JULIANDAY(CURRENT_DATE) - JULIANDAY(br.due_date)) * 2
        AS INTEGER
    ) AS estimated_fine
FROM borrowings br
INNER JOIN books b ON br.book_id = b.book_id
INNER JOIN members m ON br.member_id = m.member_id
WHERE br.return_date IS NULL AND br.due_date < CURRENT_DATE
ORDER BY days_overdue DESC
"""
pd.read_sql_query(query2_2, conn).to_csv("task2_2.csv", index=False)

# ─────────────────────────────────────────────
# Task 3.1
# ─────────────────────────────────────────────
query3_1 = """
WITH member_borrowing_stats AS (
    SELECT
        m.member_id,
        m.name AS member_name,
        m.membership_type,
        COUNT(br.borrow_id) AS total_borrowings,
        COUNT(br.return_date) AS books_returned,
        COUNT(br.borrow_id) - COUNT(br.return_date) AS books_still_borrowed,
        COALESCE(SUM(br.fine_amount), 0) AS total_fines_paid
    FROM members m
    LEFT JOIN borrowings br
        ON m.member_id = br.member_id
    GROUP BY m.member_id, m.name, m.membership_type
),
return_performance AS (
    SELECT
        br.member_id,
        SUM(
            CASE 
                WHEN br.return_date IS NOT NULL 
                     AND br.return_date <= br.due_date
                THEN 1
                ELSE 0
            END
        ) AS on_time_returns
    FROM borrowings br
    GROUP BY br.member_id
)
SELECT
    mbs.member_name,
    mbs.membership_type,
    mbs.total_borrowings,
    mbs.books_returned,
    mbs.books_still_borrowed,
    mbs.total_fines_paid,
    ROUND(
        CASE 
            WHEN mbs.total_borrowings = 0 THEN 0
            ELSE (COALESCE(rp.on_time_returns,0) * 100.0 
                 / mbs.total_borrowings)
        END,
        2
    ) AS on_time_return_rate,
    CASE
        WHEN mbs.total_borrowings = 0 THEN 'Inactive'
        WHEN mbs.total_borrowings BETWEEN 1 AND 5 THEN 'Active'
        ELSE 'Very Active'
    END AS member_category
FROM member_borrowing_stats mbs
LEFT JOIN return_performance rp
    ON mbs.member_id = rp.member_id
ORDER BY mbs.total_borrowings DESC
"""
pd.read_sql_query(query3_1, conn).to_csv("task3_1.csv", index=False)

conn.close()
