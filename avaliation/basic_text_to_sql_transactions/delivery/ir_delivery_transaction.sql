-- Results for delivery transation ** Basic Text-to-SQL



---------------------
-- 1.1 - DEEPSEEK  --
---------------------
CREATE OR REPLACE FUNCTION bmsql_proc_delivery_bg(
    p_w_id INTEGER,
    p_o_carrier_id INTEGER
) RETURNS TABLE(
    d_id INTEGER,
    o_id INTEGER,
    skipped BOOLEAN
) AS $$
DECLARE
    v_d_id INTEGER;
    v_no_o_id INTEGER;
    v_o_c_id INTEGER;
    v_ol_total NUMERIC(12,2);
    v_current_timestamp TIMESTAMP;
    v_skipped BOOLEAN;
BEGIN
    -- Get current timestamp for delivery date
    v_current_timestamp := CURRENT_TIMESTAMP;
    
    -- Process one order for each of the 10 districts
    FOR v_d_id IN 1..10 LOOP
        v_skipped := false;
        
        -- Start a database transaction for each order (as per specification flexibility)
        BEGIN
            -- Find the oldest undelivered order for this district
            SELECT no_o_id INTO v_no_o_id
            FROM bmsql_new_order
            WHERE no_w_id = p_w_id 
              AND no_d_id = v_d_id
            ORDER BY no_o_id ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED;  -- Skip locked rows to avoid deadlocks
            
            -- If no order found, skip this district
            IF NOT FOUND THEN
                v_skipped := true;
                -- Return skipped district information
                d_id := v_d_id;
                o_id := NULL;
                skipped := true;
                RETURN NEXT;
                CONTINUE;
            END IF;
            
            -- Delete from new_order table
            DELETE FROM bmsql_new_order
            WHERE no_w_id = p_w_id 
              AND no_d_id = v_d_id 
              AND no_o_id = v_no_o_id;
            
            -- Update order with carrier info and get customer ID
            UPDATE bmsql_orders
            SET o_carrier_id = p_o_carrier_id
            WHERE o_w_id = p_w_id 
              AND o_d_id = v_d_id 
              AND o_id = v_no_o_id
            RETURNING o_c_id INTO v_o_c_id;
            
            -- Update order_lines with delivery date and calculate total amount
            WITH updated_lines AS (
                UPDATE bmsql_order_line
                SET ol_delivery_d = v_current_timestamp
                WHERE ol_w_id = p_w_id 
                  AND ol_d_id = v_d_id 
                  AND ol_o_id = v_no_o_id
                RETURNING ol_amount
            )
            SELECT COALESCE(SUM(ol_amount), 0) INTO v_ol_total
            FROM updated_lines;
            
            -- Update customer balance and delivery count
            UPDATE bmsql_customer
            SET c_balance = c_balance + v_ol_total,
                c_delivery_cnt = c_delivery_cnt + 1
            WHERE c_w_id = p_w_id 
              AND c_d_id = v_d_id 
              AND c_id = v_o_c_id;
            
            -- Commit this order's transaction implicitly (each iteration is separate)
            -- Return successful delivery information
            d_id := v_d_id;
            o_id := v_no_o_id;
            skipped := false;
            RETURN NEXT;
            
        EXCEPTION
            WHEN OTHERS THEN
                -- Log error and continue with next district
                RAISE WARNING 'Error processing district %: %', v_d_id, SQLERRM;
                v_skipped := true;
                d_id := v_d_id;
                o_id := NULL;
                skipped := true;
                RETURN NEXT;
        END;
    END LOOP;
    
    RETURN;
END;
$$ LANGUAGE plpgsql;

--------------------
-- 1.2 - CHATGPT  --
--------------------
CREATE OR REPLACE FUNCTION bmsql_proc_delivery_bg(
    p_w_id         INTEGER,
    p_o_carrier_id INTEGER
)
RETURNS VOID
LANGUAGE plpgsql
AS $func$
DECLARE
    d               INTEGER;
    v_no_o_id       INTEGER;
    v_o_c_id        INTEGER;
    v_ol_amount_sum NUMERIC;
    v_now           TIMESTAMPTZ := clock_timestamp();
BEGIN
    -- Loop over districts 1..10
    FOR d IN 1..10 LOOP

        --------------------------------------------------------------------
        -- 1) Find the oldest NEW_ORDER for this district (SKIP LOCKED)
        --------------------------------------------------------------------
        SELECT no_o_id
          INTO v_no_o_id
          FROM new_order
         WHERE no_w_id = p_w_id
           AND no_d_id = d
         ORDER BY no_o_id
         LIMIT 1
         FOR UPDATE SKIP LOCKED;

        -- If no outstanding order => skip
        IF v_no_o_id IS NULL THEN
            CONTINUE;
        END IF;

        --------------------------------------------------------------------
        -- 2) Delete NEW_ORDER row
        --------------------------------------------------------------------
        DELETE FROM new_order
         WHERE no_w_id = p_w_id
           AND no_d_id = d
           AND no_o_id = v_no_o_id;

        --------------------------------------------------------------------
        -- 3) Get ORDER row and update carrier
        --------------------------------------------------------------------
        SELECT o_c_id
          INTO v_o_c_id
          FROM orders
         WHERE o_w_id = p_w_id
           AND o_d_id = d
           AND o_id   = v_no_o_id
         FOR UPDATE;

        UPDATE orders
           SET o_carrier_id = p_o_carrier_id
         WHERE o_w_id = p_w_id
           AND o_d_id = d
           AND o_id   = v_no_o_id;

        --------------------------------------------------------------------
        -- 4) Sum ORDER_LINE amounts and set delivery timestamp
        --------------------------------------------------------------------
        SELECT COALESCE(SUM(ol_amount),0)
          INTO v_ol_amount_sum
          FROM order_line
         WHERE ol_w_id = p_w_id
           AND ol_d_id = d
           AND ol_o_id = v_no_o_id;

        UPDATE order_line
           SET ol_delivery_d = clock_timestamp()
         WHERE ol_w_id = p_w_id
           AND ol_d_id = d
           AND ol_o_id = v_no_o_id;

        --------------------------------------------------------------------
        -- 5) Update CUSTOMER balance and delivery count
        --------------------------------------------------------------------
        UPDATE customer
           SET c_balance      = c_balance + v_ol_amount_sum,
               c_delivery_cnt = c_delivery_cnt + 1
         WHERE c_w_id = p_w_id
           AND c_d_id = d
           AND c_id   = v_o_c_id;

        --------------------------------------------------------------------
        -- 6) Record the result into tpcc_delivery_result table
        --------------------------------------------------------------------
        INSERT INTO tpcc_delivery_result(
            queued_ts,
            completed_ts,
            w_id,
            o_carrier_id,
            d_id,
            o_id
        )
        VALUES (
            v_now,
            clock_timestamp(),
            p_w_id,
            p_o_carrier_id,
            d,
            v_no_o_id
        );

    END LOOP;

    RAISE NOTICE 'Delivery done for warehouse %, carrier %', p_w_id, p_o_carrier_id;
END;
$func$;


-------------------
-- 1.3 - GEMINI  --
-------------------
CREATE OR REPLACE FUNCTION tpcc_delivery(
    p_w_id INT,           -- Home Warehouse ID
    p_o_carrier_id INT    -- Carrier ID (randomly selected [1..10])
)
RETURNS TABLE(
    district_id INT,
    order_id INT,
    customer_id INT,
    delivery_skipped BOOLEAN
)
LANGUAGE plpgsql
AS $$
DECLARE
    -- Variables to hold data from the transaction steps
    d_id_loop INT;
    v_no_o_id INT;
    v_o_c_id INT;
    v_ol_total_amount NUMERIC(6, 2);
    v_delivery_date TIMESTAMP := now();
    v_skipped BOOLEAN := FALSE;
BEGIN
    -- This outer BEGIN/END block is the function body.
    -- The transaction is typically managed by the calling client (or implicit auto-commit).

    -- Loop through all 10 districts (D_ID 1 to 10)
    FOR d_id_loop IN 1..10 LOOP
        -- Start a sub-block to handle potential "no outstanding order" exceptions
        BEGIN
            v_skipped := FALSE;
            v_ol_total_amount := 0;

            -- 1. Find the oldest undelivered order for the district
            SELECT no_o_id INTO v_no_o_id
            FROM new_order
            WHERE no_w_id = p_w_id AND no_d_id = d_id_loop
            ORDER BY no_o_id ASC
            LIMIT 1
            FOR UPDATE; -- Lock the row to prevent concurrent delivery

            -- If no row is found, v_no_o_id will be NULL, and we skip this district.
            IF v_no_o_id IS NULL THEN
                v_skipped := TRUE;
                -- Use a custom SQLSTATE 'P0001' for clarity in the exception handler
                RAISE EXCEPTION 'No outstanding order for W=% D=%', p_w_id, d_id_loop
                    USING ERRCODE = 'P0001';
            END IF;

            -- 2. Delete the selected row from the NEW-ORDER table
            DELETE FROM new_order
            WHERE no_w_id = p_w_id
              AND no_d_id = d_id_loop
              AND no_o_id = v_no_o_id;

            -- 3. Retrieve O_C_ID and update O_CARRIER_ID in the ORDER table
            UPDATE "order" -- "order" is a reserved word, double-quotes are needed
            SET o_carrier_id = p_o_carrier_id
            WHERE o_w_id = p_w_id
              AND o_d_id = d_id_loop
              AND o_id = v_no_o_id
            RETURNING o_c_id INTO v_o_c_id;

            -- 4. Update OL_DELIVERY_D and calculate the sum of OL_AMOUNT
            WITH updated_lines AS (
                UPDATE order_line
                SET ol_delivery_d = v_delivery_date
                WHERE ol_w_id = p_w_id
                  AND ol_d_id = d_id_loop
                  AND ol_o_id = v_no_o_id
                RETURNING ol_amount
            )
            SELECT COALESCE(SUM(ol_amount), 0) INTO v_ol_total_amount
            FROM updated_lines;

            -- 5. Update the CUSTOMER's balance and delivery count
            UPDATE customer
            SET c_balance = c_balance + v_ol_total_amount,
                c_delivery_cnt = c_delivery_cnt + 1
            WHERE c_w_id = p_w_id
              AND c_d_id = d_id_loop
              AND c_id = v_o_c_id;

            -- Return the result for the delivered order
            RETURN NEXT;

        EXCEPTION
            -- Handle the custom exception for 'No outstanding order'
            WHEN SQLSTATE 'P0001' THEN
                -- If we skipped an order, we still need to record the result.
                v_skipped := TRUE;
                v_no_o_id := NULL; -- Order ID is unknown/non-existent
                v_o_c_id := NULL;  -- Customer ID is unknown
                RETURN NEXT;
            -- For other errors (deadlocks, etc.), the transaction will likely roll back.
            WHEN OTHERS THEN
                RAISE;

        END; -- End of the inner BEGIN/EXCEPTION/END block
    END LOOP;

    RETURN;
END;
$$;