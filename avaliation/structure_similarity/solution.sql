CREATE OR REPLACE FUNCTION bmsql_proc_stock_level(
    IN w_id INT,
    IN d_id INT,
    IN threshold INT
)
RETURNS TABLE (low_stock_count BIGINT)
AS $$
DECLARE
    next_o_id INT;
BEGIN
    -- 1. Get D_NEXT_O_ID from the DISTRICT table
    SELECT D_NEXT_O_ID INTO next_o_id
    FROM DISTRICT
    WHERE D_W_ID = w_id AND D_ID = d_id;

    -- 2. and 3. Find distinct items from the last 20 orders whose stock is below the threshold
    RETURN QUERY
    WITH recent_order_items AS (
        -- Select the distinct item IDs (OL_I_ID) for the last 20 orders
        SELECT DISTINCT OL_I_ID
        FROM ORDER_LINE
        WHERE
            OL_W_ID = w_id
            AND OL_D_ID = d_id
            -- Orders from D_NEXT_O_ID - 20 up to, but not including, D_NEXT_O_ID
            AND OL_O_ID < next_o_id
            AND OL_O_ID >= (next_o_id - 20)
    )
    -- Count the number of distinct items from the last 20 orders 
    -- whose stock level (S_QUANTITY) at the home warehouse (S_W_ID = W_ID) is below the threshold.
    SELECT COUNT(T1.OL_I_ID) AS low_stock_count
    FROM recent_order_items AS T1
    JOIN STOCK AS T2 ON
        T2.S_I_ID = T1.OL_I_ID
        AND T2.S_W_ID = w_id
    WHERE
        T2.S_QUANTITY < threshold;

END;
$$ LANGUAGE plpgsql;