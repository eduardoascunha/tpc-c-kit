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
-- Helper composite type for passing order lines
CREATE TYPE tpcc_new_order_line AS (
  ol_number       integer,   -- 1..ol_cnt
  ol_i_id         integer,   -- item id
  ol_supply_w_id  integer,   -- supplying warehouse id
  ol_quantity     integer    -- quantity ordered
);

-- Main New-Order transaction procedure
CREATE OR REPLACE FUNCTION bmsql_proc_new_order(
  p_w_id   integer,
  p_d_id   integer,
  p_c_id   integer,
  p_items tpcc_new_order_line[],
  p_rbk   boolean DEFAULT false  -- set true when simulating the 1% bad-last-item case
)
RETURNS TABLE (
  o_id            integer,
  o_entry_d       timestamptz,
  o_ol_cnt        integer,
  c_last          text,
  c_credit        text,
  c_discount      numeric,
  w_tax           numeric,
  d_tax           numeric,
  total_amount    numeric,
  exec_status     text    -- NULL on success; not used when function raises exception for rollback
)
LANGUAGE plpgsql
AS $bmsql$
DECLARE
  v_now          timestamptz := now();
  v_ol_cnt       integer := COALESCE(array_length(p_items,1),0);
  v_next_o_id    integer;
  v_o_id         integer;
  v_w_tax        numeric;
  v_d_tax        numeric;
  v_c_discount   numeric;
  v_c_last       text;
  v_c_credit     text;
  v_sum_ol_amt   numeric := 0;
  v_i            integer;
  v_item_price   numeric;
  v_item_name    text;
  v_item_data    text;
  v_s_quantity   integer;
  v_s_ytd        integer;
  v_s_order_cnt  integer;
  v_s_remote_cnt integer;
  v_s_data       text;
  v_s_dist_xx    text;
  v_ol_amount    numeric;
  v_all_local    boolean := true;
BEGIN
  IF v_ol_cnt < 1 THEN
    RAISE EXCEPTION 'No order lines provided';
  END IF;

  -- 1) Read warehouse tax (shared lock)
  SELECT w_tax INTO v_w_tax
  FROM warehouse
  WHERE w_id = p_w_id
  FOR SHARE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Warehouse % not found', p_w_id;
  END IF;

  -- 2) Read and increment district next order id (must be locked)
  SELECT d_tax, d_next_o_id
    INTO v_d_tax, v_next_o_id
  FROM district
  WHERE d_w_id = p_w_id AND d_id = p_d_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'District %/% not found', p_w_id, p_d_id;
  END IF;

  v_o_id := v_next_o_id;

  UPDATE district
    SET d_next_o_id = d_next_o_id + 1
    WHERE d_w_id = p_w_id AND d_id = p_d_id;

  -- 3) Read customer
  SELECT c_discount, c_last, c_credit
    INTO v_c_discount, v_c_last, v_c_credit
  FROM customer
  WHERE c_w_id = p_w_id AND c_d_id = p_d_id AND c_id = p_c_id
  FOR SHARE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Customer %/%/% not found', p_w_id, p_d_id, p_c_id;
  END IF;

  -- 4) Insert into orders (O_CARRIER_ID NULL). We'll update o_all_local later.
  INSERT INTO orders (
    o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local
  ) VALUES (
    v_o_id, p_d_id, p_w_id, p_c_id, v_now, NULL, v_ol_cnt, 1
  );

  -- 5) Insert into new_order
  INSERT INTO new_order (no_o_id, no_d_id, no_w_id)
  VALUES (v_o_id, p_d_id, p_w_id);

  -- 6) Process each order line
  FOR i IN 1..v_ol_cnt LOOP
    -- fetch item info
    SELECT i_price, i_name, i_data
      INTO v_item_price, v_item_name, v_item_data
    FROM item
    WHERE i_id = (p_items[i]).ol_i_id;

    IF NOT FOUND THEN
      -- If last item and p_rbk is true, this is the special rollback condition
      IF i = v_ol_cnt AND p_rbk THEN
        -- Per TPC-C spec: signal not-found to cause rollback
        RAISE EXCEPTION 'Item number is not valid';
      ELSE
        RAISE EXCEPTION 'Item % not found (line %)', (p_items[i]).ol_i_id, i;
      END IF;
    END IF;

    -- select and lock stock row for this item/supplying warehouse
    SELECT s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data,
      CASE p_d_id
        WHEN 1 THEN s_dist_01 WHEN 2 THEN s_dist_02 WHEN 3 THEN s_dist_03 WHEN 4 THEN s_dist_04 WHEN 5 THEN s_dist_05
        WHEN 6 THEN s_dist_06 WHEN 7 THEN s_dist_07 WHEN 8 THEN s_dist_08 WHEN 9 THEN s_dist_09 WHEN 10 THEN s_dist_10
      END
      INTO v_s_quantity, v_s_ytd, v_s_order_cnt, v_s_remote_cnt, v_s_data, v_s_dist_xx
    FROM stock
    WHERE s_i_id = (p_items[i]).ol_i_id AND s_w_id = (p_items[i]).ol_supply_w_id
    FOR UPDATE;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'Stock for item % at warehouse % not found', (p_items[i]).ol_i_id, (p_items[i]).ol_supply_w_id;
    END IF;

    -- Update stock quantity per TPC-C rules
    IF v_s_quantity > (p_items[i]).ol_quantity + 10 THEN
      UPDATE stock
      SET s_quantity = s_quantity - (p_items[i]).ol_quantity,
          s_ytd = s_ytd + (p_items[i]).ol_quantity,
          s_order_cnt = s_order_cnt + 1,
          s_remote_cnt = s_remote_cnt + CASE WHEN (p_items[i]).ol_supply_w_id <> p_w_id THEN 1 ELSE 0 END
      WHERE s_i_id = (p_items[i]).ol_i_id AND s_w_id = (p_items[i]).ol_supply_w_id;
    ELSE
      UPDATE stock
      SET s_quantity = (s_quantity - (p_items[i]).ol_quantity) + 91,
          s_ytd = s_ytd + (p_items[i]).ol_quantity,
          s_order_cnt = s_order_cnt + 1,
          s_remote_cnt = s_remote_cnt + CASE WHEN (p_items[i]).ol_supply_w_id <> p_w_id THEN 1 ELSE 0 END
      WHERE s_i_id = (p_items[i]).ol_i_id AND s_w_id = (p_items[i]).ol_supply_w_id;
    END IF;

    -- compute OL_AMOUNT and accumulate
    v_ol_amount := (p_items[i]).ol_quantity * v_item_price;
    v_sum_ol_amt := v_sum_ol_amt + v_ol_amount;

    -- brand/generic check not stored in this schema field; computed only for terminal display usually
    -- v_brand_generic := CASE WHEN position('ORIGINAL' in upper(coalesce(v_item_data,''))) > 0
    --                         AND position('ORIGINAL' in upper(coalesce(v_s_data,''))) > 0 THEN 'B' ELSE 'G' END;

    -- insert order_line
    INSERT INTO order_line (
      ol_o_id, ol_d_id, ol_w_id, ol_number,
      ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info, ol_delivery_d
    ) VALUES (
      v_o_id, p_d_id, p_w_id, i,
      (p_items[i]).ol_i_id, (p_items[i]).ol_supply_w_id, (p_items[i]).ol_quantity, v_ol_amount, v_s_dist_xx, NULL
    );

    -- track whether all lines are local
    IF (p_items[i]).ol_supply_w_id <> p_w_id THEN
      v_all_local := false;
    END IF;
  END LOOP;

  -- 7) compute total amount
  total_amount := v_sum_ol_amt * (1 - v_c_discount) * (1 + v_w_tax + v_d_tax);

  -- 8) update orders.o_all_local to correct value (in case any remote lines)
  UPDATE orders
    SET o_all_local = CASE WHEN v_all_local THEN 1 ELSE 0 END
    WHERE o_id = v_o_id;

  -- 9) populate RETURN values
  o_id := v_o_id;
  o_entry_d := v_now;
  o_ol_cnt := v_ol_cnt;
  c_last := v_c_last;
  c_credit := v_c_credit;
  c_discount := v_c_discount;
  w_tax := v_w_tax;
  d_tax := v_d_tax;
  exec_status := NULL;

  RETURN NEXT;
  RETURN;

EXCEPTION
  WHEN OTHERS THEN
    -- If this is the special 'Item number is not valid' TPC-C case, re-raise exactly that message so the caller/harness sees it and the DB transaction rolls back.
    IF SQLERRM LIKE '%Item number is not valid%' THEN
      RAISE EXCEPTION 'Item number is not valid';
    ELSE
      RAISE;
    END IF;
END;
$bmsql$;


-------------------
-- 1.3 - GEMINI  --
-------------------
-- 1. Create the Composite Type (Must be run first, possibly as a separate command)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'order_item_details') THEN
        CREATE TYPE order_item_details AS (
            ol_i_id         INT,
            ol_supply_w_id  INT,
            ol_quantity     INT
        );
    END IF;
END
$$ LANGUAGE plpgsql;
-- Note the semicolon terminating the DO block above.

-- 2. Create the Function (Must be run as a single statement)
CREATE OR REPLACE FUNCTION bmsql_proc_new_order(
    w_id_param          INT,        -- Home Warehouse ID
    d_id_param          INT,        -- District ID
    c_id_param          INT,        -- Customer ID
    ol_cnt_param        INT,        -- Number of items (ol_cnt)
    items_param         order_item_details[] -- Array of items to order
)
RETURNS TABLE(
    o_id_out            INT,
    total_amount_out    NUMERIC(12,2),
    execution_status    TEXT
)
AS $$
DECLARE
    w_tax_val           NUMERIC(4,4);
    d_tax_val           NUMERIC(4,4);
    d_next_o_id_val     INT;
    c_discount_val      NUMERIC(4,4);
    c_last_val          VARCHAR(16);
    c_credit_val        VARCHAR(2);
    o_entry_d_val       TIMESTAMP;
    o_all_local_val     INT := 1;
    total_amount_val    NUMERIC(12,2) := 0;
    i_loop              INT;
    current_item        order_item_details;
    ol_i_id_val         INT;
    ol_supply_w_id_val  INT;
    ol_quantity_val     INT;
    i_price_val         NUMERIC(5,2);
    i_name_val          VARCHAR(24);
    i_data_val          VARCHAR(50);
    s_quantity_val      INT;
    s_dist_xx_val       VARCHAR(24);
    s_data_val          VARCHAR(50);
    ol_amount_val       NUMERIC(6,2);
    brand_generic_val   CHAR(1);
    rollback_needed     BOOLEAN := FALSE;
    rollback_item_pos   INT := -1;
BEGIN
    -- ... (Core PL/pgSQL logic follows) ...

    -- 2. Select WAREHOUSE data
    SELECT W_TAX INTO w_tax_val
    FROM WAREHOUSE
    WHERE W_ID = w_id_param;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Warehouse % not found', w_id_param;
    END IF;

    -- 3. Select and Update DISTRICT data
    SELECT D_TAX, D_NEXT_O_ID INTO d_tax_val, d_next_o_id_val
    FROM DISTRICT
    WHERE D_W_ID = w_id_param AND D_ID = d_id_param
    FOR UPDATE;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'District %/% not found', w_id_param, d_id_param;
    END IF;
    
    UPDATE DISTRICT
    SET D_NEXT_O_ID = D_NEXT_O_ID + 1
    WHERE D_W_ID = w_id_param AND D_ID = d_id_param;
    
    o_id_out := d_next_o_id_val;

    -- 4. Select CUSTOMER data
    SELECT C_DISCOUNT, C_LAST, C_CREDIT INTO c_discount_val, c_last_val, c_credit_val
    FROM CUSTOMER
    WHERE C_W_ID = w_id_param AND C_D_ID = d_id_param AND C_ID = c_id_param;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Customer %/%/% not found', w_id_param, d_id_param, c_id_param;
    END IF;

    -- 5. Determine O_ALL_LOCAL
    FOREACH current_item IN ARRAY items_param
    LOOP
        IF current_item.ol_supply_w_id <> w_id_param THEN
            o_all_local_val := 0;
            EXIT;
        END IF;
    END LOOP;

    -- 6. Insert into ORDER and NEW-ORDER tables
    o_entry_d_val := CURRENT_TIMESTAMP;
    
    INSERT INTO "ORDER" (
        O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL
    ) VALUES (
        o_id_out, d_id_param, w_id_param, c_id_param, o_entry_d_val, NULL, ol_cnt_param, o_all_local_val
    );

    INSERT INTO NEW_ORDER (
        NO_O_ID, NO_D_ID, NO_W_ID
    ) VALUES (
        o_id_out, d_id_param, w_id_param
    );

    -- 7. Process each item (Order-Line)
    FOR i_loop IN 1..ol_cnt_param LOOP
        current_item := items_param[i_loop];
        ol_i_id_val := current_item.ol_i_id;
        ol_supply_w_id_val := current_item.ol_supply_w_id;
        ol_quantity_val := current_item.ol_quantity;
        
        -- 7a. Select ITEM data
        SELECT I_PRICE, I_NAME, I_DATA INTO i_price_val, i_name_val, i_data_val
        FROM ITEM
        WHERE I_ID = ol_i_id_val;

        IF NOT FOUND THEN
            rollback_needed := TRUE;
            rollback_item_pos := i_loop;
            execution_status := 'Item number is not valid';
            CONTINUE;
        END IF;
        
        -- 7b. Select and Update STOCK data
        SELECT S_QUANTITY, 
               CASE d_id_param 
                   WHEN 1 THEN S_DIST_01 WHEN 2 THEN S_DIST_02 WHEN 3 THEN S_DIST_03 
                   WHEN 4 THEN S_DIST_04 WHEN 5 THEN S_DIST_05 WHEN 6 THEN S_DIST_06 
                   WHEN 7 THEN S_DIST_07 WHEN 8 THEN S_DIST_08 WHEN 9 THEN S_DIST_09 
                   WHEN 10 THEN S_DIST_10 
               END,
               S_DATA
        INTO s_quantity_val, s_dist_xx_val, s_data_val
        FROM STOCK
        WHERE S_I_ID = ol_i_id_val AND S_W_ID = ol_supply_w_id_val
        FOR UPDATE;

        -- Update S_QUANTITY logic
        IF s_quantity_val >= (ol_quantity_val + 10) THEN
            s_quantity_val := s_quantity_val - ol_quantity_val;
        ELSE
            s_quantity_val := s_quantity_val - ol_quantity_val + 91;
        END IF;

        -- Update STOCK table
        UPDATE STOCK
        SET 
            S_QUANTITY = s_quantity_val,
            S_YTD = S_YTD + ol_quantity_val,
            S_ORDER_CNT = S_ORDER_CNT + 1,
            S_REMOTE_CNT = S_REMOTE_CNT + CASE WHEN ol_supply_w_id_val <> w_id_param THEN 1 ELSE 0 END
        WHERE S_I_ID = ol_i_id_val AND S_W_ID = ol_supply_w_id_val;

        -- 7c. Compute OL_AMOUNT
        ol_amount_val := ol_quantity_val * i_price_val;

        -- 7d. Determine Brand/Generic field
        IF (i_data_val LIKE '%ORIGINAL%') AND (s_data_val LIKE '%ORIGINAL%') THEN
            brand_generic_val := 'B';
        ELSE
            brand_generic_val := 'G';
        END IF;

        -- 7e. Insert into ORDER-LINE table
        INSERT INTO ORDER_LINE (
            OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, 
            OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO
        ) VALUES (
            o_id_out, d_id_param, w_id_param, i_loop, ol_i_id_val, ol_supply_w_id_val, 
            ol_quantity_val, ol_amount_val, s_dist_xx_val
        );

        -- 7f. Add to total
        total_amount_val := total_amount_val + ol_amount_val;
        
    END LOOP;

    -- 8. Final calculations and Commit/Rollback
    total_amount_out := total_amount_val * (1 - c_discount_val) * (1 + w_tax_val + d_tax_val);

    IF rollback_needed THEN
        ROLLBACK;
        -- execution_status is already set to 'Item number is not valid'
    ELSE
        -- Commit is implicit if ROLLBACK is not called
        execution_status := 'Success';
    END IF;
    
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;
