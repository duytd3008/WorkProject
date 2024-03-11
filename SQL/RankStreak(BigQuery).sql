WITH TABLE_1 AS (
        SELECT month_M, client_id, nhom_KH, revenue_M1_per_revenue_M  FROM `ad_hoc.IDKH_PercentageRevenue`
        WHERE nhom_KH like 'A%' AND
        month_M BETWEEN (SELECT DATE_SUB(MAX(month_M), INTERVAL 11 month) FROM `ad_hoc.IDKH_PercentageRevenue`)
        AND (SELECT MAX(month_M) FROM `ghn-cxcscc.ad_hoc.IDKH_PercentageRevenue`)
      ),
      TABLE_2 AS (
        SELECT L.*,
        r.month_M as PreviousMonth, r.nhom_KH as PreviousMonthRank 
        FROM TABLE_1 AS L
        LEFT JOIN TABLE_1 AS R
        ON L.client_id = R.client_id
        AND l.month_M = date_add(r.month_M, interval 1 month)
        order by l.month_M, l.client_id
      ),
      TABLE_STREAK_COND_1 AS (
        SELECT *, 
        (case when PreviousMonthRank is null then 1 else 0 end) as streak_change  
        FROM TABLE_2
      ),
      TABLE_STREAK_STATUS_COND_1 AS (
        SELECT *, SUM(streak_change) OVER (PARTITION BY client_id order by month_M) as streak_status FROM TABLE_STREAK_COND_1
        ORDER BY month_M asc
      ),
      TABLE_STREAK_DURATION_COND_1 AS (
        SELECT month_M, client_id, nhom_KH, 
        PreviousMonth, PreviousMonthRank, 
        ROW_NUMBER() OVER (PARTITION BY client_id, streak_status order by month_M) as StreakDuration 
        FROM TABLE_STREAK_STATUS_COND_1
        ORDER BY client_id, month_M
      ),
      TABLE_COND_1 AS (
        SELECT *,
        (CASE 
        WHEN StreakDuration = 12 THEN 'Group1'
        WHEN StreakDuration < 12 AND StreakDuration >= 9 THEN 'Group2'
        WHEN StreakDuration < 9 AND StreakDuration >= 6 THEN 'Group3'
        WHEN StreakDuration < 6 AND StreakDuration >= 3 THEN 'Group4'
        ELSE 'None'
        END
        ) AS Cond_1_status
         FROM TABLE_STREAK_DURATION_COND_1
        WHERE month_M = DATE '2024-03-01'
      ),
      TABLE_COND_1_FILTER AS (
      SELECT * FROM TABLE_COND_1
      WHERE Cond_1_status in ('Group1', 'Group2', 'Group3', 'Group4')
      ),
      TABLE_MERGE_1 AS (
        SELECT L.*,
        (CASE WHEN R1.revenue_M1_per_revenue_M IS NULL THEN 0 ELSE CAST(R1.revenue_M1_per_revenue_M AS BIGNUMERIC) END) AS Month_sub_1_RR,
        (CASE WHEN R2.revenue_M1_per_revenue_M IS NULL THEN 0 ELSE CAST(R2.revenue_M1_per_revenue_M AS BIGNUMERIC) END) AS Month_sub_2_RR,
        (CASE WHEN R3.revenue_M1_per_revenue_M IS NULL THEN 0 ELSE CAST(R3.revenue_M1_per_revenue_M AS BIGNUMERIC) END) AS Month_sub_3_RR

        FROM TABLE_COND_1_FILTER AS L
        LEFT JOIN TABLE_1 AS R1
        ON L.month_M = DATE_ADD(R1.month_M, interval 2 month)
        AND L.client_id = R1.client_id

        LEFT JOIN TABLE_1 AS R2
        ON L.month_M = DATE_ADD(R2.month_M, interval 3 month)
        AND L.client_id = R2.client_id

        LEFT JOIN TABLE_1 AS R3
        ON L.month_M = DATE_ADD(R3.month_M, interval 4 month)
        AND L.client_id = R3.client_id

      ),
      TABLE_MERGE_2 AS (
        SELECT month_M, client_id, nhom_KH, StreakDuration, Cond_1_status, 
        (select round(avg(Estimate), 2) 
        from unnest([Month_sub_1_RR, Month_sub_2_RR, Month_sub_3_RR]) Estimate
        ) as AVG_RR
        FROM TABLE_MERGE_1

      ),
      TABLE_MERGE_3 AS (
        SELECT *, 
        (
        CASE 
        WHEN AVG_RR >= 100 then 'Group1'
        WHEN AVG_RR >= 90 AND AVG_RR < 100 then 'Group2'
        WHEN AVG_RR >= 80 AND AVG_RR < 90 then 'Group3'
        WHEN AVG_RR < 80 then 'Group4'
        END
        
        ) AS Cond_2_status
         FROM TABLE_MERGE_2
      ),
      TABLE_MERGE_4 AS (
        SELECT *, RANK() OVER (ORDER BY Cond_1_status asc, Cond_2_status asc) as RankNum FROM TABLE_MERGE_3
      )
SELECT * FROM TABLE_MERGE_4
ORDER BY StreakDuration desc, AVG_RR desc, RankNum asc
