-- Results for new order transation ** Basic Text-to-SQL


---------------------
-- 1.1 - DEEPSEEK  --
---------------------
CREATE OR REPLACE FUNCTION bmsql_proc_new_order(
    p_w_id INTEGER,
    p_d_id INTEGER,
    p_c_id INTEGER,
    p_ol_cnt INTEGER,
    p_item_ids INTEGER[],
    p_supply_w_ids INTEGER[],
    p_quantities INTEGER[],
    p_order_date TIMESTAMP DEFAULT NOW()
) RETURNS TABLE(
    w_id INTEGER,
    d_id INTEGER,
    c_id INTEGER,
    o_id INTEGER,
    o_ol_cnt INTEGER,
    c_last VARCHAR,
    c_credit VARCHAR,
    c_discount NUMERIC,
    w_tax NUMERIC,
    d_tax NUMERIC,
    o_entry_d TIMESTAMP,
    total_amount NUMERIC,
    execution_status VARCHAR,
    item_details JSONB
) AS $$
DECLARE
    v_w_tax NUMERIC;
    v_d_tax NUMERIC;
    v_d_next_o_id INTEGER;
    v_c_discount NUMERIC;
    v_c_last VARCHAR;
    v_c_credit VARCHAR;
    v_o_id INTEGER;
    v_o_all_local INTEGER := 1;
    v_total_amount NUMERIC := 0;
    v_ol_amount NUMERIC;
    v_i_price NUMERIC;
    v_i_name VARCHAR;
    v_i_data VARCHAR;
    v_s_quantity INTEGER;
    v_s_data VARCHAR;
    v_s_dist_xx VARCHAR;
    v_brand_generic CHAR;
    v_ol_number INTEGER;
    v_item_not_found BOOLEAN := FALSE;
    v_rbk INTEGER;
    v_remote_warehouse_exists BOOLEAN;
    v_item_details JSONB[];
BEGIN
    -- Check for rollback condition (1% of transactions)
    v_rbk := (CASE WHEN random() < 0.01 THEN 1 ELSE 0 END);
    
    -- If this is a rollback transaction and it's the last item, set to unused value
    IF v_rbk = 1 AND p_ol_cnt > 0 THEN
        p_item_ids[p_ol_cnt] := -1; -- Unused item ID that will cause not-found
    END IF;

    -- Step 1: Get warehouse tax rate
    SELECT w_tax INTO v_w_tax
    FROM warehouse
    WHERE w_id = p_w_id;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Warehouse % not found', p_w_id;
    END IF;

    -- Step 2: Get district tax rate and next order ID (with update)
    SELECT d_tax, d_next_o_id 
    INTO v_d_tax, v_d_next_o_id
    FROM district
    WHERE d_w_id = p_w_id AND d_id = p_d_id
    FOR UPDATE;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'District %, % not found', p_w_id, p_d_id;
    END IF;
    
    v_o_id := v_d_next_o_id;
    
    -- Update next order ID
    UPDATE district 
    SET d_next_o_id = d_next_o_id + 1
    WHERE d_w_id = p_w_id AND d_id = p_d_id;

    -- Step 3: Get customer information
    SELECT c_discount, c_last, c_credit
    INTO v_c_discount, v_c_last, v_c_credit
    FROM customer
    WHERE c_w_id = p_w_id AND c_d_id = p_d_id AND c_id = p_c_id;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Customer %, %, % not found', p_w_id, p_d_id, p_c_id;
    END IF;

    -- Step 4: Check if any order lines are remote
    SELECT EXISTS(
        SELECT 1 
        FROM unnest(p_supply_w_ids) AS supp_w_id 
        WHERE supp_w_id != p_w_id
    ) INTO v_remote_warehouse_exists;
    
    v_o_all_local := CASE WHEN v_remote_warehouse_exists THEN 0 ELSE 1 END;

    -- Step 5: Insert into ORDER and NEW-ORDER tables
    INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
    VALUES (v_o_id, p_d_id, p_w_id, p_c_id, p_order_date, p_ol_cnt, v_o_all_local);

    INSERT INTO new_order (no_o_id, no_d_id, no_w_id)
    VALUES (v_o_id, p_d_id, p_w_id);

    -- Initialize item details array
    v_item_details := array[]::JSONB[];

    -- Step 6: Process each order line
    FOR v_ol_number IN 1..p_ol_cnt LOOP
        -- Check for unused item (rollback condition)
        IF p_item_ids[v_ol_number] = -1 THEN
            v_item_not_found := TRUE;
            -- Skip remaining processing for this item as per spec
            CONTINUE;
        END IF;

        -- Get item information
        SELECT i_price, i_name, i_data
        INTO v_i_price, v_i_name, v_i_data
        FROM item
        WHERE i_id = p_item_ids[v_ol_number];
        
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Item % not found', p_item_ids[v_ol_number];
        END IF;

        -- Get stock information and update
        SELECT s_quantity, s_data,
               CASE p_d_id 
                   WHEN 1 THEN s_dist_01
                   WHEN 2 THEN s_dist_02
                   WHEN 3 THEN s_dist_03
                   WHEN 4 THEN s_dist_04
                   WHEN 5 THEN s_dist_05
                   WHEN 6 THEN s_dist_06
                   WHEN 7 THEN s_dist_07
                   WHEN 8 THEN s_dist_08
                   WHEN 9 THEN s_dist_09
                   WHEN 10 THEN s_dist_10
               END AS s_dist_xx
        INTO v_s_quantity, v_s_data, v_s_dist_xx
        FROM stock
        WHERE s_i_id = p_item_ids[v_ol_number] 
          AND s_w_id = p_supply_w_ids[v_ol_number]
        FOR UPDATE;
        
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Stock for item %, warehouse % not found', 
                p_item_ids[v_ol_number], p_supply_w_ids[v_ol_number];
        END IF;

        -- Update stock quantity
        IF v_s_quantity >= p_quantities[v_ol_number] + 10 THEN
            UPDATE stock 
            SET s_quantity = s_quantity - p_quantities[v_ol_number],
                s_ytd = s_ytd + p_quantities[v_ol_number],
                s_order_cnt = s_order_cnt + 1,
                s_remote_cnt = s_remote_cnt + CASE WHEN p_supply_w_ids[v_ol_number] != p_w_id THEN 1 ELSE 0 END
            WHERE s_i_id = p_item_ids[v_ol_number] 
              AND s_w_id = p_supply_w_ids[v_ol_number];
        ELSE
            UPDATE stock 
            SET s_quantity = s_quantity - p_quantities[v_ol_number] + 91,
                s_ytd = s_ytd + p_quantities[v_ol_number],
                s_order_cnt = s_order_cnt + 1,
                s_remote_cnt = s_remote_cnt + CASE WHEN p_supply_w_ids[v_ol_number] != p_w_id THEN 1 ELSE 0 END
            WHERE s_i_id = p_item_ids[v_ol_number] 
              AND s_w_id = p_supply_w_ids[v_ol_number];
        END IF;

        -- Calculate order line amount
        v_ol_amount := p_quantities[v_ol_number] * v_i_price;
        v_total_amount := v_total_amount + v_ol_amount;

        -- Determine brand generic
        IF v_i_data LIKE '%ORIGINAL%' AND v_s_data LIKE '%ORIGINAL%' THEN
            v_brand_generic := 'B';
        ELSE
            v_brand_generic := 'G';
        END IF;

        -- Insert order line
        INSERT INTO order_line (
            ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id,
            ol_quantity, ol_amount, ol_dist_info
        ) VALUES (
            v_o_id, p_d_id, p_w_id, v_ol_number, p_item_ids[v_ol_number], 
            p_supply_w_ids[v_ol_number], p_quantities[v_ol_number], 
            v_ol_amount, v_s_dist_xx
        );

        -- Collect item details for output
        v_item_details := array_append(
            v_item_details,
            jsonb_build_object(
                'ol_number', v_ol_number,
                'ol_supply_w_id', p_supply_w_ids[v_ol_number],
                'ol_i_id', p_item_ids[v_ol_number],
                'i_name', v_i_name,
                'ol_quantity', p_quantities[v_ol_number],
                's_quantity', v_s_quantity,
                'brand_generic', v_brand_generic,
                'i_price', v_i_price,
                'ol_amount', v_ol_amount
            )
        );
    END LOOP;

    -- Calculate final total amount
    v_total_amount := v_total_amount * (1 - v_c_discount) * (1 + v_w_tax + v_d_tax);

    -- Handle rollback for unused item
    IF v_item_not_found THEN
        ROLLBACK;
        execution_status := 'Item number is not valid';
        -- Return minimal data as per spec for rolled back transactions
        RETURN QUERY SELECT 
            p_w_id, p_d_id, p_c_id, v_o_id, p_ol_cnt, v_c_last, v_c_credit,
            v_c_discount, v_w_tax, v_d_tax, p_order_date, v_total_amount,
            execution_status, NULL::JSONB;
    ELSE
        COMMIT;
        execution_status := 'Order successfully placed';
        -- Convert array to single JSONB object for return
        item_details := (SELECT jsonb_agg(elem) FROM unnest(v_item_details) AS elem);
        RETURN QUERY SELECT 
            p_w_id, p_d_id, p_c_id, v_o_id, p_ol_cnt, v_c_last, v_c_credit,
            v_c_discount, v_w_tax, v_d_tax, p_order_date, v_total_amount,
            execution_status, item_details;
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;
$$ LANGUAGE plpgsql;

--------------------
-- 1.3 - CHATGPT  --
--------------------

-------------------
-- 1.3 - GEMINI  --
-------------------
