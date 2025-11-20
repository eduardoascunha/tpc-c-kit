-- Results for stock level transation ** Basic Text-to-SQL

------------------------
-- ORIGINAL PROCEDURE --
------------------------
CREATE OR REPLACE FUNCTION bmsql_proc_stock_level(
    IN in_w_id integer,
    IN in_d_id integer,
    IN in_threshold integer,
    OUT out_low_stock integer
) AS
$$
BEGIN
    SELECT INTO out_low_stock
			count(*) AS low_stock
		FROM (
			SELECT s_w_id, s_i_id, s_quantity
			FROM bmsql_stock
			WHERE s_w_id = in_w_id AND s_quantity < in_threshold
			  AND s_i_id IN (
				SELECT ol_i_id
					FROM bmsql_district
					JOIN bmsql_order_line ON ol_w_id = d_w_id
					 AND ol_d_id = d_id
					 AND ol_o_id >= d_next_o_id - 20
					 AND ol_o_id < d_next_o_id
					WHERE d_w_id = in_w_id AND d_id = in_d_id
				)
			) AS L;
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION bmsql_proc_delivery_bg(
	IN in_w_id integer,
	IN in_o_carrier_id integer,
	IN in_ol_delivery_d timestamp,
	OUT out_delivered_o_id integer[]
) AS
$$
DECLARE
	var_d_id integer;
	var_o_id integer;
	var_c_id integer;
	var_sum_ol_amount decimal(12, 2);
BEGIN
	FOR var_d_id IN 1..10 LOOP
		var_o_id = -1;
		/*
		 * Try to find the oldest undelivered order for this
		 * DISTRICT. There may not be one, which is a case
		 * that needs to be reported.
		*/
		WHILE var_o_id < 0 LOOP
			SELECT INTO var_o_id
					no_o_id
				FROM bmsql_new_order
			WHERE no_w_id = in_w_id AND no_d_id = var_d_id
			ORDER BY no_o_id ASC;
			IF NOT FOUND THEN
			    var_o_id = -1;
				EXIT;
			END IF;

			DELETE FROM bmsql_new_order
				WHERE no_w_id = in_w_id AND no_d_id = var_d_id
				  AND no_o_id = var_o_id;
			IF NOT FOUND THEN
			    var_o_id = -1;
			END IF;
		END LOOP;

		IF var_o_id < 0 THEN
			-- No undelivered NEW_ORDER found for this District.
			var_d_id = var_d_id + 1;
			CONTINUE;
		END IF;

		/*
		 * We found out oldert undelivered order for this DISTRICT
		 * and the NEW_ORDER line has been deleted. Process the
		 * rest of the DELIVERY_BG.
		*/

		-- Update the ORDER setting the o_carrier_id.
		UPDATE bmsql_oorder
			SET o_carrier_id = in_o_carrier_id
			WHERE o_w_id = in_w_id AND o_d_id = var_d_id AND o_id = var_o_id;

		-- Get the o_c_id from the ORDER.
		SELECT INTO var_c_id
				o_c_id
			FROM bmsql_oorder
			WHERE o_w_id = in_w_id AND o_d_id = var_d_id AND o_id = var_o_id;

		-- Update ORDER_LINE setting the ol_delivery_d.
		UPDATE bmsql_order_line
			SET ol_delivery_d = in_ol_delivery_d
			WHERE ol_w_id = in_w_id AND ol_d_id = var_d_id
			  AND ol_o_id = var_o_id;

		-- SELECT the sum(ol_amount) from ORDER_LINE.
		SELECT INTO var_sum_ol_amount
				sum(ol_amount) AS sum_ol_amount
			FROM bmsql_order_line
			WHERE ol_w_id = in_w_id AND ol_d_id = var_d_id
			  AND ol_o_id = var_o_id;

		-- Update the CUSTOMER.
		UPDATE bmsql_customer
			SET c_balance = c_balance + var_sum_ol_amount,
				c_delivery_cnt = c_delivery_cnt + 1
			WHERE c_w_id = in_w_id AND c_d_id = var_d_id and c_id = var_c_id;

		out_delivered_o_id[var_d_id] = var_o_id;

		var_d_id = var_d_id +1 ;
	END LOOP;
END;
$$
LANGUAGE plpgsql;


---------------------
-- 1.1 - DEEPSEEK  --
---------------------
-- Stock-Level Transaction for TPC-C
CREATE OR REPLACE FUNCTION stock_level_transaction(
    p_w_id INTEGER,
    p_d_id INTEGER,
    p_threshold INTEGER
) RETURNS INTEGER AS $$
DECLARE
    v_d_next_o_id INTEGER;
    v_low_stock INTEGER;
    v_start_o_id INTEGER;
BEGIN
    -- Step 1: Get the next available order number from DISTRICT
    SELECT d_next_o_id INTO v_d_next_o_id
    FROM district
    WHERE d_w_id = p_w_id AND d_id = p_d_id;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'District not found: W_ID=%, D_ID=%', p_w_id, p_d_id;
    END IF;
    
    -- Calculate the starting order ID (last 20 orders)
    v_start_o_id := v_d_next_o_id - 20;
    
    -- Step 2 & 3: Count distinct items with stock below threshold from last 20 orders
    SELECT COUNT(DISTINCT s_i_id) INTO v_low_stock
    FROM (
        -- Get distinct items from the last 20 orders
        SELECT DISTINCT ol_i_id
        FROM order_line
        WHERE ol_w_id = p_w_id 
          AND ol_d_id = p_d_id 
          AND ol_o_id >= v_start_o_id 
          AND ol_o_id < v_d_next_o_id
    ) AS recent_items
    JOIN stock ON s_i_id = ol_i_id AND s_w_id = p_w_id
    WHERE s_quantity < p_threshold;
    
    RETURN v_low_stock;
    
EXCEPTION
    WHEN OTHERS THEN
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Alternative: Single query version (more efficient)
CREATE OR REPLACE FUNCTION stock_level_transaction_single_query(
    p_w_id INTEGER,
    p_d_id INTEGER,
    p_threshold INTEGER
) RETURNS INTEGER AS $$
DECLARE
    v_low_stock INTEGER;
BEGIN
    WITH recent_items AS (
        -- Get distinct items from the last 20 orders
        SELECT DISTINCT ol_i_id
        FROM order_line
        WHERE ol_w_id = p_w_id 
          AND ol_d_id = p_d_id 
          AND ol_o_id >= (
              SELECT d_next_o_id - 20 
              FROM district 
              WHERE d_w_id = p_w_id AND d_id = p_d_id
          )
          AND ol_o_id < (
              SELECT d_next_o_id 
              FROM district 
              WHERE d_w_id = p_w_id AND d_id = p_d_id
          )
    )
    SELECT COUNT(*) INTO v_low_stock
    FROM recent_items ri
    JOIN stock s ON s.s_i_id = ri.ol_i_id AND s.s_w_id = p_w_id
    WHERE s.s_quantity < p_threshold;
    
    RETURN v_low_stock;
END;
$$ LANGUAGE plpgsql;

-- Complete transaction with output display simulation
CREATE OR REPLACE FUNCTION execute_stock_level(
    p_w_id INTEGER,
    p_d_id INTEGER,
    p_threshold INTEGER
) RETURNS TABLE(
    warehouse_num INTEGER,
    district_num INTEGER,
    threshold_level INTEGER,
    low_stock_count INTEGER
) AS $$
DECLARE
    v_low_stock INTEGER;
BEGIN
    -- Validate threshold range (10-20 as per TPC-C spec)
    IF p_threshold < 10 OR p_threshold > 20 THEN
        RAISE EXCEPTION 'Threshold must be between 10 and 20, got %', p_threshold;
    END IF;
    
    -- Execute the stock level transaction
    SELECT stock_level_transaction_single_query(p_w_id, p_d_id, p_threshold) 
    INTO v_low_stock;
    
    -- Return results in the format expected for terminal display
    RETURN QUERY SELECT 
        p_w_id AS warehouse_num,
        p_d_id AS district_num,
        p_threshold AS threshold_level,
        v_low_stock AS low_stock_count;
        
END;
$$ LANGUAGE plpgsql;

-- Example usage:
-- SELECT * FROM execute_stock_level(1, 1, 15);

-- For direct use in application code:
/*
DO $$
DECLARE
    w_id INTEGER := 1;
    d_id INTEGER := 1;
    threshold INTEGER := 15;
    low_stock INTEGER;
BEGIN
    SELECT stock_level_transaction(w_id, d_id, threshold) INTO low_stock;
    
    -- Display output in TPC-C terminal format
    RAISE NOTICE 'Stock-Level';
    RAISE NOTICE 'Warehouse: %', w_id;
    RAISE NOTICE 'District: %', d_id;
    RAISE NOTICE 'Stock Level Threshold: %', threshold;
    RAISE NOTICE 'low stock: %', low_stock;
END $$;
*/

--------------------
-- 1.2 - CHATGPT  --
--------------------
-- Input:
--   :w_id       -- warehouse id
--   :d_id       -- district id
--   :threshold  -- stock minimum threshold (10–20)

BEGIN;

-- 1. Retrieve the district’s next order id
SELECT d_next_o_id
INTO STRICT next_o_id
FROM district
WHERE d_w_id = :w_id
  AND d_id   = :d_id;

-- Define order range: last 20 orders
LET lower_o_id = next_o_id - 20;

-- 2. Get distinct item IDs from the last 20 orders
WITH recent_items AS (
    SELECT DISTINCT ol_i_id
    FROM order_line
    WHERE ol_w_id = :w_id
      AND ol_d_id = :d_id
      AND ol_o_id <  next_o_id
      AND ol_o_id >= lower_o_id
),

-- 3. Count stocks below threshold for these items
low_items AS (
    SELECT COUNT(*) AS low_stock_count
    FROM stock s
    JOIN recent_items ri
      ON s.s_i_id = ri.ol_i_id
    WHERE s.s_w_id = :w_id
      AND s.s_quantity < :threshold
)

SELECT low_stock_count
FROM low_items;

COMMIT;


-------------------
-- 1.3 - GEMINI  --
-------------------
-- Function to execute the TPC-C Stock-Level transaction
CREATE OR REPLACE FUNCTION tpc_c_stock_level(
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
    -- Since full serializability isn't required, we can select with a non-locking read.
    SELECT D_NEXT_O_ID INTO next_o_id
    FROM DISTRICT
    WHERE D_W_ID = w_id AND D_ID = d_id;

    -- Note: TPC-C requires the transaction to potentially be broken into multiple
    -- database transactions, and repeatable reads aren't needed (Clause 2.8.2.3).
    -- However, for simplicity and performance in a single database transaction,
    -- we use a single query that follows all the required steps:

    -- 2. and 3. Find distinct items from the last 20 orders whose stock is below the threshold
    RETURN QUERY
    WITH recent_order_items AS (
        -- Select the distinct item IDs (OL_I_ID) from the ORDER-LINE table
        -- for the last 20 orders (OL_O_ID >= D_NEXT_O_ID - 20 AND OL_O_ID < D_NEXT_O_ID)
        SELECT DISTINCT OL_I_ID
        FROM ORDER_LINE
        WHERE
            OL_W_ID = w_id
            AND OL_D_ID = d_id
            AND OL_O_ID < next_o_id
            AND OL_O_ID >= (next_o_id - 20)
    )
    -- Count the number of distinct items (from the CTE)
    -- whose stock level (S_QUANTITY) at the home warehouse (S_W_ID = W_ID)
    -- is below the specified threshold.
    SELECT COUNT(T1.OL_I_ID) AS low_stock_count
    FROM recent_order_items AS T1
    JOIN STOCK AS T2 ON
        T2.S_I_ID = T1.OL_I_ID
        AND T2.S_W_ID = w_id
    WHERE
        T2.S_QUANTITY < threshold;

END;
$$ LANGUAGE plpgsql;

-- **Example Execution:**
-- SELECT * FROM tpc_c_stock_level(1, 1, 15);