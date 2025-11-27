-- Results for delivery transation ** Basic Text-to-SQL

------------------------
-- ORIGINAL PROCEDURE --
------------------------
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
-- Delivery Transaction for TPC-C
-- Processes 10 orders (one per district) for a given warehouse

CREATE OR REPLACE FUNCTION delivery_transaction(
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
            FROM new_order
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
            DELETE FROM new_order
            WHERE no_w_id = p_w_id 
              AND no_d_id = v_d_id 
              AND no_o_id = v_no_o_id;
            
            -- Update order with carrier info and get customer ID
            UPDATE orders
            SET o_carrier_id = p_o_carrier_id
            WHERE o_w_id = p_w_id 
              AND o_d_id = v_d_id 
              AND o_id = v_no_o_id
            RETURNING o_c_id INTO v_o_c_id;
            
            -- Update order_lines with delivery date and calculate total amount
            WITH updated_lines AS (
                UPDATE order_line
                SET ol_delivery_d = v_current_timestamp
                WHERE ol_w_id = p_w_id 
                  AND ol_d_id = v_d_id 
                  AND ol_o_id = v_no_o_id
                RETURNING ol_amount
            )
            SELECT COALESCE(SUM(ol_amount), 0) INTO v_ol_total
            FROM updated_lines;
            
            -- Update customer balance and delivery count
            UPDATE customer
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

CREATE OR REPLACE PROCEDURE tpcc_delivery(
    IN p_w_id         int,
    IN p_o_carrier_id int,
    IN p_commit_each  boolean DEFAULT false  -- if true: commit after each order; otherwise a single transaction for all districts
)
LANGUAGE plpgsql
AS $$
DECLARE
    d              int;
    v_no_o_id      bigint;
    v_o_c_id       bigint;
    v_ol_amount_sum numeric;
    v_now          timestamptz;
    v_skipped      int := 0;
BEGIN
    -- Record the queued time once (procedure entry). Per TPC-C, queued time should be recorded when enqueuing;
    -- here we approximate it as the start time of processing.
    v_now := clock_timestamp();

    -- If commit-per-order mode: start a transaction which we will commit inside the loop.
    IF p_commit_each THEN
        -- Start an explicit transaction; inside a CALL context we may use transaction control commands.
        -- We'll start a transaction for the first order; if the caller already started a transaction,
        -- this will be a no-op in practice — ensure callers CALL this procedure without an outer BEGIN/COMMIT
        -- if p_commit_each = true.
        START TRANSACTION;
    END IF;

    FOR d IN 1..10 LOOP
        /*
         * 1) Choose the oldest NEW_ORDER for this (W,D).
         *    Use FOR UPDATE SKIP LOCKED so concurrent delivery workers don't race on same NEW_ORDER row.
         *    Ordering by no_o_id ensures the lowest (oldest) order number is chosen.
         */
        SELECT no_o_id
        INTO v_no_o_id
        FROM new_order
        WHERE no_w_id = p_w_id
          AND no_d_id = d
        ORDER BY no_o_id
        LIMIT 1
        FOR UPDATE SKIP LOCKED;

        IF v_no_o_id IS NULL THEN
            -- no outstanding order in that district -> skip it
            v_skipped := v_skipped + 1;
            CONTINUE;
        END IF;

        /*
         * 2) Delete the NEW_ORDER row (removes order from new-order list).
         *    We already locked it with FOR UPDATE SKIP LOCKED so delete will succeed for this worker.
         */
        DELETE FROM new_order
        WHERE no_w_id = p_w_id
          AND no_d_id = d
          AND no_o_id = v_no_o_id;

        /*
         * 3) Select ORDER row, get O_C_ID and update O_CARRIER_ID
         */
        SELECT o_c_id
        INTO v_o_c_id
        FROM orders
        WHERE o_w_id = p_w_id
          AND o_d_id = d
          AND o_id = v_no_o_id
        FOR UPDATE;  -- lock the order row while we update it

        UPDATE orders
        SET o_carrier_id = p_o_carrier_id
        WHERE o_w_id = p_w_id
          AND o_d_id = d
          AND o_id = v_no_o_id;

        /*
         * 4) Update ORDER_LINE rows: set OL_DELIVERY_D = now(), and compute sum of OL_AMOUNT
         */
        SELECT COALESCE(SUM(ol_amount), 0)
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

        /*
         * 5) Update CUSTOMER: add sum to C_BALANCE, increment C_DELIVERY_CNT
         */
        UPDATE customer
        SET c_balance = c_balance + v_ol_amount_sum,
            c_delivery_cnt = c_delivery_cnt + 1
        WHERE c_w_id = p_w_id
          AND c_d_id = d
          AND c_id = v_o_c_id;

        /*
         * 6) Record delivered order into the result table (the "result file").
         *    Use the processing completion time for completed_ts.
         *    TPC-C requires the result file to include: queued time, W_ID, O_CARRIER_ID, D_ID and O_ID, and completion time.
         */
        INSERT INTO tpcc_delivery_result(queued_ts, completed_ts, w_id, o_carrier_id, d_id, o_id)
        VALUES (v_now, clock_timestamp(), p_w_id, p_o_carrier_id, d, v_no_o_id);

        /*
         * 7) Optionally commit this DB transaction now (if p_commit_each).
         *    When COMMIT is executed inside a stored procedure it commits the transaction context.
         *    To continue processing the next order as a fresh transaction, start a new transaction immediately.
         */
        IF p_commit_each THEN
            COMMIT;
            -- start a new tx for the next iteration (unless it was the last)
            IF d < 10 THEN
                START TRANSACTION;
            END IF;
        END IF;

    END LOOP;

    -- If we used single-transaction mode, we just return and the caller's transaction will commit.
    -- If we used commit-each mode and the last loop left a transaction open, commit it now.
    IF p_commit_each THEN
        -- If the last iteration didn't commit (possible if we never delivered any order),
        -- the transaction is still open; commit it to leave DB clean.
        -- (If everything was committed in-loop, this will commit the final transaction.)
        COMMIT;
    END IF;

    -- Optionally emit a NOTICE with summary (useful for logs)
    RAISE NOTICE 'tpcc_delivery: warehouse %, carrier %, districts skipped: %', p_w_id, p_o_carrier_id, v_skipped;
END;
$$;


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
    -- Loop variable for district
    d_id_loop INT;
    -- Variables to hold data from the transaction steps
    v_no_o_id INT;
    v_o_c_id INT;
    v_ol_total_amount NUMERIC(6, 2);
    v_delivery_date TIMESTAMP := now();
    v_skipped BOOLEAN := FALSE;
BEGIN
    -- The TPC-C specification requires that the Delivery transaction
    -- processes one outstanding order for each of the 10 districts (D_ID 1 to 10).
    FOR d_id_loop IN 1..10 LOOP
        -- Start a sub-block to handle potential "no outstanding order" exceptions
        BEGIN
            v_skipped := FALSE;
            v_ol_total_amount := 0;

            -- 1. Find the oldest undelivered order for the district
            -- The FOR UPDATE clause ensures exclusive lock on the selected row
            -- and makes sure the SELECT and DELETE are atomic within the transaction.
            SELECT no_o_id INTO v_no_o_id
            FROM new_order
            WHERE no_w_id = p_w_id AND no_d_id = d_id_loop
            ORDER BY no_o_id ASC
            LIMIT 1
            FOR UPDATE;

            -- If no row is found, v_no_o_id will be NULL, and we skip this district.
            IF v_no_o_id IS NULL THEN
                v_skipped := TRUE;
                RAISE EXCEPTION 'No outstanding order for W=% D=%', p_w_id, d_id_loop;
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
            -- A CTE (Common Table Expression) is used here to select and update
            -- in a single statement.
            WITH updated_lines AS (
                UPDATE order_line
                SET ol_delivery_d = v_delivery_date
                WHERE ol_w_id = p_w_id
                  AND ol_d_id = d_id_loop
                  AND ol_o_id = v_no_o_id
                RETURNING ol_amount
            )
            SELECT SUM(ol_amount) INTO v_ol_total_amount
            FROM updated_lines;

            -- 5. Update the CUSTOMER's balance and delivery count
            UPDATE customer
            SET c_balance = c_balance + v_ol_total_amount,
                c_delivery_cnt = c_delivery_cnt + 1
            WHERE c_w_id = p_w_id
              AND c_d_id = d_id_loop
              AND c_id = v_o_c_id;

            -- Return the result for the delivered order
            RETURN QUERY SELECT d_id_loop, v_no_o_id, v_o_c_id, v_skipped;

        EXCEPTION
            -- Catch the "No outstanding order" exception to skip the district
            WHEN OTHERS THEN
                -- Per TPC-C spec: "The condition in which no outstanding order is
                -- present at a given district must be handled by skipping the
                -- delivery of an order for that district only and resuming the
                -- delivery of an order from all remaining districts..."
                -- Note: The RAISE EXCEPTION call above is caught here.
                -- Other exceptions (e.g., deadlocks) would be re-raised by default
                -- after a warning/log, and this whole transaction would roll back.
                IF SQLSTATE = 'P0001' THEN -- Custom Exception for 'No outstanding order'
                    v_skipped := TRUE;
                    v_no_o_id := NULL; -- Ensure O_ID is NULL for skipped
                    v_o_c_id := NULL; -- Ensure C_ID is NULL for skipped
                    -- Return the result for the skipped order
                    RETURN QUERY SELECT d_id_loop, v_no_o_id, v_o_c_id, v_skipped;
                ELSE
                    -- For any other critical database error, re-raise it
                    RAISE;
                END IF;

        END;

    END LOOP;

    RETURN;
END;
$$;