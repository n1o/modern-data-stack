{{ config({
    "materialized": "external", 
    "location": "{{ var('bucket') }}/{{ var('categories_parquet') }}"
    }) 
}}
WITH categories_for_author AS (
SELECT 
    UNNEST(authors) author, 
    UNNEST(categories) category,
    DATEPART('year', update_date) AS publication_year
FROM {{ source('arxiv', 'arxiv_data') }}
),
grouped_categories AS (
    SELECT 
        *, 
        COUNT(*) as count 
    from categories_for_author
    WHERE author IS NOT NULL and category is not NULL
    GROUP BY ALL  
),
ordered_grouped_categories AS (
    SELECT * from grouped_categories
    ORDER BY author, publication_year DESC, count DESC
)

SELECT * FROM ordered_grouped_categories;