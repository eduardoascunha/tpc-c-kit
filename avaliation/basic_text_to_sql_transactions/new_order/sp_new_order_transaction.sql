-- Results for new order transation ** Basic Text-to-SQL

------------------------
-- ORIGINAL PROCEDURE --
------------------------
CREATE OR REPLACE FUNCTION bmsql_proc_new_order(
    IN in_w_id integer,
    IN in_d_id integer,
    IN in_c_id integer,
    IN in_ol_supply_w_id integer[],
    IN in_ol_i_id integer[],
    IN in_ol_quantity integer[],
    OUT out_w_tax decimal(4, 4),
    OUT out_d_tax decimal(4, 4),
    OUT out_o_id integer,
    OUT out_o_entry_d timestamp,
    OUT out_ol_cnt integer,
    OUT out_ol_amount decimal(12, 2)[],
    OUT out_total_amount decimal(12, 2),
    OUT out_c_last varchar(16),
    OUT out_c_credit char(2),
    OUT out_c_discount decimal(4, 4),
    OUT out_i_name varchar(24)[],
    OUT out_i_price decimal(5, 2)[],
    OUT out_s_quantity integer[],
    OUT out_brand_generic char[]
) AS
$$
DECLARE
    var_all_local integer := 1;
    var_x integer;
    var_y integer;
    var_tmp integer;
    var_seq integer[15];
    var_item_row record;
    var_stock_row record;
BEGIN
    -- The o_entry_d is now.
    out_o_entry_d := CURRENT_TIMESTAMP;
    out_total_amount := 0.00;

    -- When processing the order lines we must select the STOCK rows
    -- FOR UPDATE. This is because we must perform business logic
    -- (the juggling with the S_QUANTITY) here in the application
    -- and cannot do that in an atomic UPDATE statement while getting
    -- the original value back at the same time (UPDATE ... RETURNING
    -- may not be vendor neutral). This can lead to possible deadlocks
    -- if two transactions try to lock the same two stock rows in
    -- opposite order. To avoid that we process the order lines in
    -- the order of the order of ol_supply_w_id, ol_i_id.
    out_ol_cnt := 0;
    FOR var_x IN 1 .. array_length(in_ol_i_id, 1) LOOP
	IF in_ol_i_id[var_x] IS NOT NULL AND in_ol_i_id[var_x] <> 0 THEN
	    out_ol_cnt := out_ol_cnt + 1;
	    var_seq[var_x] = var_x;
	    IF in_ol_supply_w_id[var_x] <> in_w_id THEN
		var_all_local := 0;
	    END IF;
	END IF;
    END LOOP;
    FOR var_x IN 1 .. out_ol_cnt - 1 LOOP
	FOR var_y IN var_x + 1 .. out_ol_cnt LOOP
	    IF in_ol_supply_w_id[var_seq[var_y]] < in_ol_supply_w_id[var_seq[var_x]] THEN
	        var_tmp = var_seq[var_x];
		var_seq[var_x] = var_seq[var_y];
		var_seq[var_y] = var_tmp;
	    ELSE
	        IF in_ol_supply_w_id[var_seq[var_y]] = in_ol_supply_w_id[var_seq[var_x]]
		AND in_ol_i_id[var_seq[var_y]] < in_ol_i_id[var_seq[var_x]] THEN
		    var_tmp = var_seq[var_x];
		    var_seq[var_x] = var_seq[var_y];
		    var_seq[var_y] = var_tmp;
		END IF;
	    END IF;
	END LOOP;
    END LOOP;

    -- Retrieve the required data from DISTRICT
    SELECT INTO out_d_tax, out_o_id
    	d_tax, d_next_o_id
	FROM bmsql_district
	WHERE d_w_id = in_w_id AND d_id = in_d_id
	FOR UPDATE;

    -- Retrieve the required data from CUSTOMER and WAREHOUSE
    SELECT INTO out_w_tax, out_c_last, out_c_credit, out_c_discount
        w_tax, c_last, c_credit, c_discount
	FROM bmsql_customer
	JOIN bmsql_warehouse ON (w_id = c_w_id)
	WHERE c_w_id = in_w_id AND c_d_id = in_d_id AND c_id = in_c_id;

    -- Update the DISTRICT bumping the D_NEXT_O_ID
    UPDATE bmsql_district
        SET d_next_o_id = d_next_o_id + 1
	WHERE d_w_id = in_w_id AND d_id = in_d_id;

    -- Insert the ORDER row
    INSERT INTO bmsql_oorder (
        o_id, o_d_id, o_w_id, o_c_id, o_entry_d,
	o_ol_cnt, o_all_local)
    VALUES (
        out_o_id, in_d_id, in_w_id, in_c_id, out_o_entry_d,
	out_ol_cnt, var_all_local);

    -- Insert the NEW_ORDER row
    INSERT INTO bmsql_new_order (
        no_o_id, no_d_id, no_w_id)
    VALUES (
        out_o_id, in_d_id, in_w_id);

    -- Per ORDER_LINE
    FOR var_x IN 1 .. out_ol_cnt LOOP
	-- We process the lines in the sequence orderd by warehouse, item.
	var_y = var_seq[var_x];
	SELECT INTO var_item_row
		i_name, i_price, i_data
	    FROM bmsql_item
	    WHERE i_id = in_ol_i_id[var_y];
        IF NOT FOUND THEN
	    RAISE EXCEPTION 'Item number is not valid';
	END IF;
	-- Found ITEM
	out_i_name[var_y] = var_item_row.i_name;
	out_i_price[var_y] = var_item_row.i_price;

        SELECT INTO var_stock_row
	        s_quantity, s_data,
		s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,
		s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
	    FROM bmsql_stock
	    WHERE s_w_id = in_ol_supply_w_id[var_y]
	    AND s_i_id = in_ol_i_id[var_y]
	    FOR UPDATE;
        IF NOT FOUND THEN
	    RAISE EXCEPTION 'STOCK not found: %,%', in_ol_supply_w_id[var_y],
	    	in_ol_i_id[var_y];
	END IF;

	out_s_quantity[var_y] = var_stock_row.s_quantity;
	out_ol_amount[var_y] = out_i_price[var_y] * in_ol_quantity[var_y];
	IF var_item_row.i_data LIKE '%ORIGINAL%'
	AND var_stock_row.s_data LIKE '%ORIGINAL%' THEN
	    out_brand_generic[var_y] := 'B';
	ELSE
	    out_brand_generic[var_y] := 'G';
	END IF;
	out_total_amount = out_total_amount +
		out_ol_amount[var_y] * (1.0 - out_c_discount)
		* (1.0 + out_w_tax + out_d_tax);

	-- Update the STOCK row.
	UPDATE bmsql_stock SET
	    	s_quantity = CASE
		WHEN var_stock_row.s_quantity >= in_ol_quantity[var_y] + 10 THEN
		    var_stock_row.s_quantity - in_ol_quantity[var_y]
		ELSE
		    var_stock_row.s_quantity + 91
		END,
		s_ytd = s_ytd + in_ol_quantity[var_y],
		s_order_cnt = s_order_cnt + 1,
		s_remote_cnt = s_remote_cnt + CASE
		WHEN in_w_id <> in_ol_supply_w_id[var_y] THEN
		    1
		ELSE
		    0
		END
	    WHERE s_w_id = in_ol_supply_w_id[var_y]
	    AND s_i_id = in_ol_i_id[var_y];

	-- Insert the ORDER_LINE row.
	INSERT INTO bmsql_order_line (
	    ol_o_id, ol_d_id, ol_w_id, ol_number,
	    ol_i_id, ol_supply_w_id, ol_quantity,
	    ol_amount, ol_dist_info)
	VALUES (
	    out_o_id, in_d_id, in_w_id, var_y,
	    in_ol_i_id[var_y], in_ol_supply_w_id[var_y], in_ol_quantity[var_y],
	    out_ol_amount[var_y],
	    CASE
		WHEN in_d_id = 1 THEN var_stock_row.s_dist_01
		WHEN in_d_id = 2 THEN var_stock_row.s_dist_02
		WHEN in_d_id = 3 THEN var_stock_row.s_dist_03
		WHEN in_d_id = 4 THEN var_stock_row.s_dist_04
		WHEN in_d_id = 5 THEN var_stock_row.s_dist_05
		WHEN in_d_id = 6 THEN var_stock_row.s_dist_06
		WHEN in_d_id = 7 THEN var_stock_row.s_dist_07
		WHEN in_d_id = 8 THEN var_stock_row.s_dist_08
		WHEN in_d_id = 9 THEN var_stock_row.s_dist_09
		WHEN in_d_id = 10 THEN var_stock_row.s_dist_10
	    END);

    END LOOP;

    RETURN;
END;
$$
LANGUAGE plpgsql;

---------------------
-- 1.1 - DEEPSEEK  -- runned
---------------------
-- New-Order Transaction for TPC-C
CREATE OR REPLACE FUNCTION new_order_transaction(
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

        -- Collect item details for output (simplified - in real implementation would use arrays)
        item_details := jsonb_build_object(
            'ol_number', v_ol_number,
            'ol_supply_w_id', p_supply_w_ids[v_ol_number],
            'ol_i_id', p_item_ids[v_ol_number],
            'i_name', v_i_name,
            'ol_quantity', p_quantities[v_ol_number],
            's_quantity', v_s_quantity,
            'brand_generic', v_brand_generic,
            'i_price', v_i_price,
            'ol_amount', v_ol_amount
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
-- 1.2 - CHATGPT  --
--------------------
-- 1) helper composite type to pass lines
-- CREATE TYPE tpcc_new_order_line AS (
--   ol_number    integer,   -- line number within the order (1..ol_cnt)
--   ol_i_id      integer,   -- item id
--   ol_supply_w_id integer, -- supplying warehouse for this line (given by terminal)
--   ol_quantity  integer    -- quantity ordered (1..10)
-- );

-- 2) function implementing the New-Order transaction
CREATE OR REPLACE FUNCTION new_order_txn(
  p_w_id   integer,
  p_d_id   integer,
  p_c_id   integer,
  p_items  tpcc_new_order_line[],
  p_rbk    boolean DEFAULT false  -- set true when terminal simulated rbk=1 and last item set to unused
)
RETURNS TABLE (
  o_id        integer,
  o_entry_d   timestamptz,
  o_ol_cnt    integer,
  c_last      text,
  c_credit    text,
  c_discount  numeric,
  w_tax       numeric,
  d_tax       numeric,
  total_amount numeric,
  exec_status text -- NULL on success, 'Item number is not valid' on the defined rollback
) LANGUAGE plpgsql AS
$$
DECLARE
  v_w_tax numeric;
  v_d_tax numeric;
  v_c_discount numeric;
  v_c_last text;
  v_c_credit text;
  v_next_o_id integer;
  v_o_id integer;
  v_all_local boolean := true;
  v_sum_ol_amount numeric := 0;
  v_ol_amount numeric;
  v_i integer;
  v_item record;
  v_stock record;
  v_ol_cnt integer := COALESCE(array_length(p_items,1), 0);
  v_now timestamptz := now();
  v_brand_generic text;
  v_dist_info text;
BEGIN
  IF v_ol_cnt < 1 THEN
    RAISE EXCEPTION 'No order lines provided';
  END IF;

  -- 1) read warehouse tax
  SELECT w_tax INTO v_w_tax
  FROM warehouse
  WHERE w_id = p_w_id
  FOR SHARE;  -- read-only lock is enough per TPC-C rules for WAREHOUSE

  -- 2) read and increment district next order id (must be atomic)
  SELECT d_tax, d_next_o_id
  INTO v_d_tax, v_next_o_id
  FROM district
  WHERE d_w_id = p_w_id AND d_id = p_d_id
  FOR UPDATE; -- lock district row while incrementing

  -- increment D_NEXT_O_ID and persist
  v_o_id := v_next_o_id;
  UPDATE district
    SET d_next_o_id = d_next_o_id + 1
    WHERE d_w_id = p_w_id AND d_id = p_d_id;

  -- 3) read customer
  SELECT c_discount, c_last, c_credit
  INTO v_c_discount, v_c_last, v_c_credit
  FROM customer
  WHERE c_w_id = p_w_id AND c_d_id = p_d_id AND c_id = p_c_id
  FOR SHARE; -- it's a read of customer

  -- 4) Insert into ORDERS and NEW_ORDER
  INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local)
  VALUES (v_o_id, p_d_id, p_w_id, p_c_id, v_now, NULL, v_ol_cnt,
          CASE WHEN (SELECT bool_and(x) FROM (SELECT ( ( (p_items[i]).ol_supply_w_id = p_w_id) ) AS x, generate_series(1, v_ol_cnt) AS i) t) THEN 1 ELSE 0 END)
  RETURNING o_id INTO v_o_id;

  -- The above uses a boolean aggregation to compute O_ALL_LOCAL; but for safety recalc below:
  v_all_local := true;
  FOR i IN 1..v_ol_cnt LOOP
    IF (p_items[i]).ol_supply_w_id IS DISTINCT FROM p_w_id THEN
      v_all_local := false;
      EXIT;
    END IF;
  END LOOP;

  -- Update o_all_local in orders (defensive)
  UPDATE orders
    SET o_all_local = CASE WHEN v_all_local THEN 1 ELSE 0 END
    WHERE o_id = v_o_id AND o_d_id = p_d_id AND o_w_id = p_w_id;

  -- Insert into new_order table
  INSERT INTO new_order (no_o_id, no_d_id, no_w_id)
  VALUES (v_o_id, p_d_id, p_w_id);

  -- 5) Process each order line
  FOR i IN 1..v_ol_cnt LOOP
    -- fetch item; if not found -> if this is the last item and p_rbk = true, then raise the special exception so transaction will rollback.
    SELECT i_price, i_name, i_data
    INTO v_item
    FROM item
    WHERE i_id = (p_items[i]).ol_i_id;

    IF NOT FOUND THEN
      IF i = v_ol_cnt AND p_rbk THEN
        -- Special rollback condition per TPC-C: signal not-found for unused last item
        RAISE EXCEPTION 'Item number is not valid';
      ELSE
        -- Unexpected missing item
        RAISE EXCEPTION 'Item id % not found (line %)', (p_items[i]).ol_i_id, i;
      END IF;
    END IF;

    -- select and lock stock row for this item/supplying warehouse
    SELECT s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data,
           -- s_dist_01..s_dist_10 columns: fetch relevant one dynamically using CASE
           CASE (p_d_id)
             WHEN 1 THEN s_dist_01 WHEN 2 THEN s_dist_02 WHEN 3 THEN s_dist_03 WHEN 4 THEN s_dist_04 WHEN 5 THEN s_dist_05
             WHEN 6 THEN s_dist_06 WHEN 7 THEN s_dist_07 WHEN 8 THEN s_dist_08 WHEN 9 THEN s_dist_09 WHEN 10 THEN s_dist_10
           END AS s_dist_xx
    INTO v_stock
    FROM stock
    WHERE s_i_id = (p_items[i]).ol_i_id AND s_w_id = (p_items[i]).ol_supply_w_id
    FOR UPDATE;

    IF NOT FOUND THEN
      -- In case stock row is missing (should not happen for valid item/warehouse combos)
      RAISE EXCEPTION 'Stock row not found for item % at warehouse %', (p_items[i]).ol_i_id, (p_items[i]).ol_supply_w_id;
    END IF;

    -- update S_QUANTITY according to the rule:
    IF v_stock.s_quantity > (p_items[i]).ol_quantity + 10 THEN
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

    -- compute ol_amount
    v_ol_amount := (p_items[i]).ol_quantity * v_item.i_price;
    v_sum_ol_amount := v_sum_ol_amount + v_ol_amount;

    -- determine brand/generic: both I_DATA and S_DATA contain 'ORIGINAL' -> 'B' else 'G'
    IF position('ORIGINAL' in upper(coalesce(v_item.i_data,''))) > 0
       AND position('ORIGINAL' in upper(coalesce(v_stock.s_data,''))) > 0 THEN
      v_brand_generic := 'B';
    ELSE
      v_brand_generic := 'G';
    END IF;

    v_dist_info := v_stock.s_dist_xx;

    -- insert order-line
    INSERT INTO order_line (
      ol_o_id, ol_d_id, ol_w_id, ol_number,
      ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info, ol_delivery_d
    )
    VALUES (
      v_o_id, p_d_id, p_w_id, i,
      (p_items[i]).ol_i_id, (p_items[i]).ol_supply_w_id, (p_items[i]).ol_quantity, v_ol_amount, v_dist_info, NULL
    );

    -- keep all_local flag up-to-date
    IF (p_items[i]).ol_supply_w_id <> p_w_id THEN
      v_all_local := false;
    END IF;
  END LOOP;

  -- 6) compute total amount
  total_amount := v_sum_ol_amount * (1 - v_c_discount) * (1 + v_w_tax + v_d_tax);

  -- 7) set additional output fields
  o_id := v_o_id;
  o_entry_d := v_now;
  o_ol_cnt := v_ol_cnt;
  c_last := v_c_last;
  c_credit := v_c_credit;
  c_discount := v_c_discount;
  w_tax := v_w_tax;
  d_tax := v_d_tax;
  exec_status := NULL;

  -- Function returns a single row with these fields
  RETURN NEXT;

EXCEPTION
  WHEN others THEN
    -- If the special 'Item number is not valid' exception was raised, map message to exec_status and re-raise so transaction rolls back.
    IF SQLERRM LIKE '%Item number is not valid%' THEN
      -- Ensure the transaction is rolled back by re-raising; return the required terminal fields in the exception-carrying manner is difficult inside a run-time exception.
      -- Re-raise with the exact message required by TPC-C (caller can catch).
      RAISE EXCEPTION 'Item number is not valid';
    ELSE
      -- Re-raise other exceptions upward
      RAISE;
    END IF;
END;
$$;


-------------------
-- 1.3 - GEMINI  --
-------------------
-- Define the composite types for item details (required for passing array data)
-- DO $$
-- BEGIN
--     IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'order_item_details') THEN
--         CREATE TYPE order_item_details AS (
--             ol_i_id         INT,
--             ol_supply_w_id  INT,
--             ol_quantity     INT
--         );
--     END IF;
-- END
-- $$ LANGUAGE plpgsql;


-- Define the New-Order function
CREATE OR REPLACE FUNCTION new_order_transaction(
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
    o_all_local_val     INT := 1; -- Assume local until a remote warehouse is found
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
    rollback_item_pos   INT := -1; -- Position of the unused item, if any
BEGIN
    -- 1. START TRANSACTION (Handled implicitly by function execution block)
    -- 2. Select WAREHOUSE data
    SELECT W_TAX INTO w_tax_val
    FROM WAREHOUSE
    WHERE W_ID = w_id_param;
    
    -- Check for WAREHOUSE not found (Error handling based on TPC-C implicit assumption)
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Warehouse % not found', w_id_param;
    END IF;

    -- 3. Select and Update DISTRICT data
    SELECT D_TAX, D_NEXT_O_ID INTO d_tax_val, d_next_o_id_val
    FROM DISTRICT
    WHERE D_W_ID = w_id_param AND D_ID = d_id_param
    FOR UPDATE; -- Lock the row to ensure D_NEXT_O_ID update is safe
    
    -- Check for DISTRICT not found
    IF NOT FOUND THEN
        RAISE EXCEPTION 'District %/% not found', w_id_param, d_id_param;
    END IF;
    
    UPDATE DISTRICT
    SET D_NEXT_O_ID = D_NEXT_O_ID + 1
    WHERE D_W_ID = w_id_param AND D_ID = d_id_param;
    
    -- The new order ID is the old D_NEXT_O_ID
    o_id_out := d_next_o_id_val;

    -- 4. Select CUSTOMER data
    SELECT C_DISCOUNT, C_LAST, C_CREDIT INTO c_discount_val, c_last_val, c_credit_val
    FROM CUSTOMER
    WHERE C_W_ID = w_id_param AND C_D_ID = d_id_param AND C_ID = c_id_param;
    
    -- Check for CUSTOMER not found
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
        
        -- Check for the unused item rollback condition (TPC-C 2.4.1.5 and 2.4.2.2)
        -- Note: The item generation logic must ensure that only the last item can be unused
        
        -- 7a. Select ITEM data
        SELECT I_PRICE, I_NAME, I_DATA INTO i_price_val, i_name_val, i_data_val
        FROM ITEM
        WHERE I_ID = ol_i_id_val;

        IF NOT FOUND THEN
            -- Unused item detected: signal rollback, but process previous items first
            rollback_needed := TRUE;
            rollback_item_pos := i_loop;
            execution_status := 'Item number is not valid';
            -- TPC-C 2.4.2.3 requires skipping subsequent steps for this item
            CONTINUE;
        END IF;
        
        -- 7b. Select and Update STOCK data
        SELECT S_QUANTITY, 
               CASE d_id_param 
                   WHEN 1 THEN S_DIST_01 
                   WHEN 2 THEN S_DIST_02 
                   WHEN 3 THEN S_DIST_03 
                   WHEN 4 THEN S_DIST_04 
                   WHEN 5 THEN S_DIST_05 
                   WHEN 6 THEN S_DIST_06 
                   WHEN 7 THEN S_DIST_07 
                   WHEN 8 THEN S_DIST_08 
                   WHEN 9 THEN S_DIST_09 
                   WHEN 10 THEN S_DIST_10 
               END,
               S_DATA
        INTO s_quantity_val, s_dist_xx_val, s_data_val
        FROM STOCK
        WHERE S_I_ID = ol_i_id_val AND S_W_ID = ol_supply_w_id_val
        FOR UPDATE; -- Lock the row

        -- Update S_QUANTITY
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
    IF rollback_needed THEN
        -- TPC-C 2.4.2.2 requires computation of total-amount *before* rollback, 
        -- but TPC-C 2.4.2.3 excludes the unused item's amount from the sum.
        -- Since the loop calculated the sum correctly (skipping the unused item), 
        -- we just compute the final amount and ROLLBACK.
        
        -- Compute final total amount (sum(OL_AMOUNT) * (1 - C_DISCOUNT) * (1 + W_TAX + D_TAX))
        total_amount_out := total_amount_val * (1 - c_discount_val) * (1 + w_tax_val + d_tax_val);

        -- Rollback the transaction
        ROLLBACK;
        
        -- The output data for rollback is different (2.4.3.4), but the function
        -- returns a status to the application which handles the display.
        
    ELSE
        -- Successful transaction
        
        -- Compute final total amount
        total_amount_out := total_amount_val * (1 - c_discount_val) * (1 + w_tax_val + d_tax_val);

        -- Commit the transaction (Handled implicitly at the end of the function)
        execution_status := 'Success';
        
    END IF;
    
    -- Return the order ID and final amount/status
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;
