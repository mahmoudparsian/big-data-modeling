import marimo

__generated_with = "0.24.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    from pathlib import Path
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    return Path, SparkSession, mo


@app.cell
def _(mo):
    mo.md("""
    # 📚 Book Ratings Analytics: DuckDB $\rightarrow$ PySpark DataFrames & SQL

    This Marimo notebook ingests the `books` (10,007 rows) and `ratings` (980,112 rows) tables from `./data_stories/book_ratings/books_db.duckdb` directly into **PySpark DataFrames**, registers them as Spark SQL temporary views (`books` and `ratings`), and runs **10 analytical SQL queries** across the dataset.
    """)
    return


@app.cell
def _(Path, SparkSession):
    # =========================================================================
    # 1. Initialize SparkSession with DuckDB JDBC Driver
    # =========================================================================
    db_path = Path("./books_ratings_db.duckdb").resolve()
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB database not found at: {db_path}")

    spark = (
        SparkSession.builder
        .appName("BookRatingsDuckDBAnalytics")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        # Pulls the official DuckDB JDBC driver automatically from Maven Central
        .config("spark.jars.packages", "org.duckdb:duckdb_jdbc:1.1.3")
        .getOrCreate()
    )

    jdbc_url = f"jdbc:duckdb:{db_path}"
    jdbc_props = {
        "driver": "org.duckdb.DuckDBDriver",
        "duckdb.read_only": "true",
    }

    # Read DuckDB tables directly into PySpark DataFrames
    books_df = spark.read.jdbc(url=jdbc_url, table="books", properties=jdbc_props)
    ratings_df = spark.read.jdbc(url=jdbc_url, table="ratings", properties=jdbc_props)

    # Cache both DataFrames in JVM memory for fast multi-query execution
    books_df.cache()
    ratings_df.cache()

    # Register DataFrames as Spark SQL tables (Temp Views)
    books_df.createOrReplaceTempView("books")
    ratings_df.createOrReplaceTempView("ratings")

    print(f"Loaded books:   {books_df.count():,} rows")
    print(f"Loaded ratings: {ratings_df.count():,} rows")
    books_df.printSchema()
    ratings_df.printSchema()
    return (spark,)


@app.cell
def _(mo):
    mo.md("""
    ---
    ### Query 1: Top 15 Highest-Rated Books (Weighted / Minimum Threshold)
    Finds the highest-rated titles among books with at least **10,000 ratings**, avoiding small-sample bias while comparing global `average_rating` with `pct_favorable`.
    """)
    return


@app.cell
def _(spark):
    q1_top_books = spark.sql("""
        SELECT 
            book_id,
            title,
            authors,
            CAST(original_publication_year AS INT) AS pub_year,
            ROUND(average_rating, 3) AS avg_rating,
            ratings_count,
            ROUND(pct_favorable, 2) AS pct_favorable
        FROM books
        WHERE ratings_count >= 10000
        ORDER BY average_rating DESC, ratings_count DESC
        LIMIT 15
    """)
    q1_top_books.show(15, truncate=45)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ### Query 2: Most Polarizing Books (Highest Rating Spread & Low Favorable Share)
    Identifies widely rated books (`work_ratings_count >= 25,000`) where readers are sharply divided—comparing 1-star vs. 5-star counts alongside `rating_spread`.
    """)
    return


@app.cell
def _(spark):
    q2_polarizing = spark.sql("""
        SELECT 
            title,
            authors,
            ROUND(average_rating, 2) AS avg_rating,
            rating_spread,
            ROUND(pct_favorable, 2) AS pct_favorable,
            ratings_1,
            ratings_5,
            ROUND(ratings_1 * 100.0 / NULLIF(num_star_ratings, 0), 2) AS pct_1_star
        FROM books
        WHERE work_ratings_count >= 25000
        ORDER BY pct_1_star DESC, rating_spread DESC
        LIMIT 15
    """)
    q2_polarizing.show(15, truncate=40)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ### Query 3: Publication Decade Trends (Volume, Quality & Favorable Rate)
    Aggregates catalog output, average rating, and median/total rating volume across `publication_decade` (from 1900 onward).
    """)
    return


@app.cell
def _(spark):
    q3_decades = spark.sql("""
        SELECT 
            publication_decade,
            COUNT(*) AS total_books,
            ROUND(AVG(average_rating), 3) AS mean_avg_rating,
            ROUND(AVG(pct_favorable), 2) AS mean_pct_favorable,
            SUM(ratings_count) AS total_ratings_volume,
            ROUND(AVG(work_text_reviews_count), 0) AS avg_text_reviews
        FROM books
        WHERE publication_decade IS NOT NULL AND publication_decade >= 1900
        GROUP BY publication_decade
        ORDER BY publication_decade ASC
    """)
    q3_decades.show(20, truncate=False)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ### Query 4: Most Prolific & Consistently Acclaimed Authors
    Groups by `authors` for writers with at least **5 books** in the catalog, ranking them by mean rating and total readership.
    """)
    return


@app.cell
def _(spark):
    q4_authors = spark.sql("""
        SELECT 
            authors,
            COUNT(*) AS book_count,
            ROUND(AVG(average_rating), 3) AS avg_book_rating,
            MIN(ROUND(average_rating, 2)) AS lowest_rated_book,
            MAX(ROUND(average_rating, 2)) AS highest_rated_book,
            SUM(ratings_count) AS total_ratings
        FROM books
        WHERE authors IS NOT NULL
        GROUP BY authors
        HAVING COUNT(*) >= 5
        ORDER BY avg_book_rating DESC, total_ratings DESC
        LIMIT 15
    """)
    q4_authors.show(15, truncate=40)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ### Query 5: Language Distribution & Engagement Breakdown
    Compares catalog coverage, average rating, and text-review-to-rating engagement ratio across `language_code`.
    """)
    return


@app.cell
def _(spark):
    q5_languages = spark.sql("""
        SELECT 
            COALESCE(NULLIF(TRIM(language_code), ''), 'unknown') AS lang,
            COUNT(*) AS book_count,
            ROUND(AVG(average_rating), 3) AS avg_rating,
            SUM(ratings_count) AS total_ratings,
            ROUND(SUM(work_text_reviews_count) * 100.0 / NULLIF(SUM(work_ratings_count), 0), 2) AS review_to_rating_pct
        FROM books
        GROUP BY 1
        HAVING COUNT(*) >= 10
        ORDER BY book_count DESC
    """)
    q5_languages.show(20, truncate=False)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ### Query 6: User Rating Behavior Distribution (`ratings` Table)
    Analyzes all 980,112 individual user ratings to see how users distribute scores (1–5) and how many unique users and books appear in the interaction log.
    """)
    return


@app.cell
def _(spark):
    q6_rating_dist = spark.sql("""
        SELECT 
            rating,
            COUNT(*) AS rating_events,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_all_ratings,
            COUNT(DISTINCT user_id) AS distinct_users,
            COUNT(DISTINCT book_id) AS distinct_books
        FROM ratings
        GROUP BY rating
        ORDER BY rating ASC
    """)
    q6_rating_dist.show(truncate=False)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ### Query 7: User Personas — Harsh Critics vs. Generous Raters (Join + CTE)
    Profiles active users (`>= 50` ratings) by their personal average rating and standard deviation, classifying them into rating personas.
    """)
    return


@app.cell
def _(spark):
    q7_user_personas = spark.sql("""
        WITH user_stats AS (
            SELECT 
                user_id,
                COUNT(*) AS books_rated,
                ROUND(AVG(rating), 2) AS user_avg_rating,
                ROUND(STDDEV(rating), 2) AS user_rating_stddev
            FROM ratings
            GROUP BY user_id
            HAVING COUNT(*) >= 50
        )
        SELECT 
            CASE 
                WHEN user_avg_rating < 3.2 THEN 'Harsh Critic (< 3.2)'
                WHEN user_avg_rating <= 4.1 THEN 'Balanced Reader (3.2 - 4.1)'
                ELSE 'Generous Enthusiast (> 4.1)'
            END AS reader_persona,
            COUNT(*) AS user_count,
            ROUND(AVG(books_rated), 1) AS avg_books_rated,
            ROUND(AVG(user_avg_rating), 2) AS group_mean_rating,
            ROUND(AVG(user_rating_stddev), 2) AS group_mean_stddev
        FROM user_stats
        GROUP BY 1
        ORDER BY group_mean_rating ASC
    """)
    q7_user_personas.show(truncate=False)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ### Query 8: Sample Rating vs. Global Goodreads Rating Drift (Join)
    Joins `ratings` with `books` to compare the average score from the 980K user sample against each book's global `average_rating` (for books with `>= 200` sample ratings).
    """)
    return


@app.cell
def _(spark):
    q8_rating_drift = spark.sql("""
        SELECT 
            b.book_id,
            b.title,
            b.authors,
            COUNT(r.rating) AS sample_ratings_cnt,
            ROUND(AVG(r.rating), 3) AS sample_avg_rating,
            ROUND(b.average_rating, 3) AS global_avg_rating,
            ROUND(AVG(r.rating) - b.average_rating, 3) AS sample_vs_global_diff
        FROM ratings r
        INNER JOIN books b
            ON r.book_id = b.book_id
        GROUP BY b.book_id, b.title, b.authors, b.average_rating
        HAVING COUNT(r.rating) >= 200
        ORDER BY ABS(AVG(r.rating) - b.average_rating) DESC
        LIMIT 15
    """)
    q8_rating_drift.show(15, truncate=38)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ### Query 9: Top 3 Highest-Rated Books per Publication Decade (Window Function)
    Uses `DENSE_RANK() OVER (PARTITION BY publication_decade ORDER BY ...)` to crown the top 3 books of each modern decade (`1970s–2010s`) among books with at least `5,000` ratings.
    """)
    return


@app.cell
def _(spark):
    q9_decade_winners = spark.sql("""
        WITH ranked_by_decade AS (
            SELECT 
                publication_decade,
                title,
                authors,
                ROUND(average_rating, 3) AS avg_rating,
                ratings_count,
                DENSE_RANK() OVER (
                    PARTITION BY publication_decade 
                    ORDER BY average_rating DESC, ratings_count DESC
                ) AS decade_rank
            FROM books
            WHERE publication_decade BETWEEN 1970 AND 2019
              AND ratings_count >= 5000
        )
        SELECT 
            publication_decade,
            decade_rank,
            title,
            authors,
            avg_rating,
            ratings_count
        FROM ranked_by_decade
        WHERE decade_rank <= 3
        ORDER BY publication_decade ASC, decade_rank ASC
    """)
    q9_decade_winners.show(20, truncate=40)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ### Query 10: Co-Rated Book Recommendations ("Readers Who Loved X Also Loved Y")
    A classic self-join collaborative filtering query on `ratings`: finds pairs of books that both received **5-star ratings** from the same users most frequently, enriched with titles from `books`.
    """)
    return


@app.cell
def _(spark):
    q10_co_rated = spark.sql("""
        WITH five_star_ratings AS (
            SELECT user_id, book_id
            FROM ratings
            WHERE rating = 5
        )
        SELECT 
            b1.title AS book_a,
            b2.title AS book_b,
            COUNT(*) AS shared_5_star_fans
        FROM five_star_ratings r1
        INNER JOIN five_star_ratings r2
            ON r1.user_id = r2.user_id
           AND r1.book_id < r2.book_id
        INNER JOIN books b1 ON r1.book_id = b1.book_id
        INNER JOIN books b2 ON r2.book_id = b2.book_id
        GROUP BY b1.title, b2.title
        ORDER BY shared_5_star_fans DESC
        LIMIT 15
    """)
    q10_co_rated.show(15, truncate=42)
    return


if __name__ == "__main__":
    app.run()
