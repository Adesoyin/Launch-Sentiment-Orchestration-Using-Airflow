select * from wiki_pages wp;



WITH cte AS (
    SELECT SUM(viewcount) AS total 
    FROM wiki_pages
),
ind AS (
    SELECT
        pagename,
        SUM(viewcount) AS total_views
    FROM wiki_pages
    GROUP BY pagename
)
SELECT 
    pagename, 
    total_views,
    concat (ROUND((total_views::numeric / (SELECT total FROM cte)) * 100, 2), '%') AS pct_view
FROM Ind
ORDER BY total_views DESC;