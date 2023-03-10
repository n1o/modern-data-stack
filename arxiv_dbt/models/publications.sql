{{ config({
    "materialized": "external", 
    "location": "{{ var('bucket') }}/{{ var('publications_parquet') }}"
    }) 
}}
with author_publications_per_year as (
        SELECT 
            UNNEST(authors) author, 
            DATEPART('year', update_date) AS publication_year
        FROM {{ source('arxiv', 'arxiv_data') }}
    ),
    grouped_publications as (
        SELECT 
            author, 
            publication_year, 
            count(*) AS publications 
        FROM author_publications_per_year
        GROUP BY author, publication_year
        ORDER BY author desc, publication_year desc
    )

SELECT * FROM grouped_publications

