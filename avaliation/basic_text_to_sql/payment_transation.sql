-- Results for payment transation ** Basic Text-to-SQL

------------------------
-- ORIGINAL PROCEDURE --
------------------------
CREATE OR REPLACE FUNCTION bmsql_proc_payment(
	IN in_w_id integer,
	IN in_d_id integer,
	INOUT in_c_id integer,
	IN in_c_d_id integer,
	IN in_c_w_id integer,
	IN in_c_last varchar(16),
	IN in_h_amount decimal(6,2),
	OUT out_w_name varchar(10),
	OUT out_w_street_1 varchar(20),
	OUT out_w_street_2 varchar(20),
	OUT out_w_city varchar(20),
	OUT out_w_state char(2),
	OUT out_w_zip char(9),
	OUT out_d_name varchar(10),
	OUT out_d_street_1 varchar(20),
	OUT out_d_street_2 varchar(20),
	OUT out_d_city varchar(20),
	OUT out_d_state char(2),
	OUT out_d_zip char(9),
	OUT out_c_first varchar(16),
	OUT out_c_middle char(2),
	OUT out_c_street_1 varchar(20),
	OUT out_c_street_2 varchar(20),
	OUT out_c_city varchar(20),
	OUT out_c_state char(2),
	OUT out_c_zip char(9),
	OUT out_c_phone char(16),
	OUT out_c_since timestamp,
	OUT out_c_credit char(2),
	OUT out_c_credit_lim decimal(12,2),
	OUT out_c_discount decimal(4,4),
	OUT out_c_balance decimal(12,2),
	OUT out_c_data varchar(500),
	OUT out_h_date timestamp
) AS
$$
BEGIN
	out_h_date := CURRENT_TIMESTAMP;

	--Update the DISTRICT
	UPDATE bmsql_district
		SET d_ytd = d_ytd + in_h_amount
		WHERE d_w_id = in_w_id AND d_id = in_d_id;

	--Select the DISTRICT
	SELECT INTO out_d_name, out_d_street_1, out_d_street_2, 
		    out_d_city, out_d_state, out_d_zip
		d_name, d_street_1, d_street_2, d_city, d_state, d_zip
	    FROM bmsql_district
	    WHERE d_w_id = in_w_id AND d_id = in_d_id
	    FOR UPDATE;

	--Update the WAREHOUSE
	UPDATE bmsql_warehouse
	    SET w_ytd = w_ytd + in_h_amount
	    WHERE w_id = in_w_id;

	--Select the WAREHOUSE
	SELECT INTO out_w_name, out_w_street_1, out_w_street_2,
		    out_w_city, out_w_state, out_w_zip
		w_name, w_street_1, w_street_2, w_city, w_state, w_zip
	    FROM bmsql_warehouse
	    WHERE w_id = in_w_id
	    FOR UPDATE;

	--If C_Last is given instead of C_ID (60%), determine the C_ID.
	IF in_c_last IS NOT NULL THEN
	    in_c_id = bmsql_cid_from_clast(in_c_w_id, in_c_d_id, in_c_last);
	END IF;

	--Select the CUSTOMER
	SELECT INTO out_c_first, out_c_middle, in_c_last, out_c_street_1,
		    out_c_street_2, out_c_city, out_c_state, out_c_zip,
		    out_c_phone, out_c_since, out_c_credit, out_c_credit_lim,
		    out_c_discount, out_c_balance
		c_first, c_middle, c_last, c_street_1,
		c_street_2, c_city, c_state, c_zip,
		c_phone, c_since, c_credit, c_credit_lim,
		c_discount, c_balance
	    FROM bmsql_customer
	    WHERE c_w_id = in_c_w_id AND c_d_id = in_c_d_id AND c_id = in_c_id
	    FOR UPDATE;

	--Update the CUSTOMER
	out_c_balance = out_c_balance-in_h_amount;
	IF out_c_credit = 'GC' THEN
	    --Customer with good credit, don't update C_DATA
	    UPDATE bmsql_customer
		SET c_balance = c_balance - in_h_amount,
		    c_ytd_payment = c_ytd_payment + in_h_amount,
		    c_payment_cnt = c_payment_cnt + 1
		WHERE c_w_id = in_c_w_id AND c_d_id=in_c_d_id AND c_id=in_c_id;
	    out_c_data := '';
	ELSE
	--Customer with bad credit, need to do the C_DATA work.
	    SELECT INTO out_c_data
		    c_data
		FROM bmsql_customer
		WHERE c_w_id = in_c_w_id AND c_d_id = in_c_d_id
		  AND c_id = in_c_id;
	    out_c_data := substring('C_ID=' || in_c_id::text ||
				    ' C_D_ID=' || in_c_d_id::text ||
				    ' C_W_ID=' || in_c_w_id::text ||
				    ' D_ID=' || in_d_id::text ||
				    ' W_ID=' || in_w_id::text ||
				    ' H_AMOUNT=' || round(in_h_amount,2)::text || '   ' ||
				    out_c_data from 1 for 500);

	    UPDATE bmsql_customer
		SET c_balance = c_balance - in_h_amount,
		    c_ytd_payment = c_ytd_payment + in_h_amount,
		    c_payment_cnt = c_payment_cnt + 1,
		    c_data = out_c_data
		WHERE c_w_id = in_c_w_id AND c_d_id = in_c_d_id
		  AND c_id = in_c_id;
	END IF;

	--Insert the HISTORY row
	INSERT INTO bmsql_history (
		    h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id,
		    h_date, h_amount, h_data)
	VALUES (
		    in_c_id, in_c_d_id, in_c_w_id, in_d_id, in_w_id,
		    out_h_date, in_h_amount, out_w_name||'    '|| out_d_name
	);
END;
$$
LANGUAGE plpgsql;

---------------------
-- 1.1 - DEEPSEEK  --
---------------------
-- Payment Transaction for TPC-C
CREATE OR REPLACE FUNCTION payment_transaction(
    p_w_id INTEGER,
    p_d_id INTEGER,
    p_c_id INTEGER DEFAULT NULL,
    p_c_last VARCHAR DEFAULT NULL,
    p_c_d_id INTEGER DEFAULT NULL,
    p_c_w_id INTEGER DEFAULT NULL,
    p_h_amount DECIMAL(6,2)
) RETURNS TABLE(
    w_id INTEGER, w_street_1 VARCHAR, w_street_2 VARCHAR, w_city VARCHAR, w_state VARCHAR, w_zip VARCHAR,
    d_id INTEGER, d_street_1 VARCHAR, d_street_2 VARCHAR, d_city VARCHAR, d_state VARCHAR, d_zip VARCHAR,
    c_id INTEGER, c_first VARCHAR, c_middle VARCHAR, c_last VARCHAR, c_street_1 VARCHAR, c_street_2 VARCHAR,
    c_city VARCHAR, c_state VARCHAR, c_zip VARCHAR, c_phone VARCHAR, c_since TIMESTAMP,
    c_credit VARCHAR, c_credit_lim DECIMAL(12,2), c_discount DECIMAL(4,4), c_balance DECIMAL(12,2),
    c_data VARCHAR, h_amount DECIMAL(6,2), h_date TIMESTAMP
) AS $$
DECLARE
    v_w_name VARCHAR;
    v_w_street_1 VARCHAR;
    v_w_street_2 VARCHAR;
    v_w_city VARCHAR;
    v_w_state VARCHAR;
    v_w_zip VARCHAR;
    
    v_d_name VARCHAR;
    v_d_street_1 VARCHAR;
    v_d_street_2 VARCHAR;
    v_d_city VARCHAR;
    v_d_state VARCHAR;
    v_d_zip VARCHAR;
    
    v_c_id INTEGER;
    v_c_first VARCHAR;
    v_c_middle VARCHAR;
    v_c_last VARCHAR;
    v_c_street_1 VARCHAR;
    v_c_street_2 VARCHAR;
    v_c_city VARCHAR;
    v_c_state VARCHAR;
    v_c_zip VARCHAR;
    v_c_phone VARCHAR;
    v_c_since TIMESTAMP;
    v_c_credit VARCHAR;
    v_c_credit_lim DECIMAL(12,2);
    v_c_discount DECIMAL(4,4);
    v_c_balance DECIMAL(12,2);
    v_c_data VARCHAR;
    
    v_h_data VARCHAR;
    v_customer_count INTEGER;
    v_middle_customer_index INTEGER;
    v_new_c_data VARCHAR;
    v_history_timestamp TIMESTAMP := CURRENT_TIMESTAMP;
BEGIN
    -- Update Warehouse
    UPDATE warehouse 
    SET w_ytd = w_ytd + p_h_amount
    WHERE w_id = p_w_id
    RETURNING w_name, w_street_1, w_street_2, w_city, w_state, w_zip
    INTO v_w_name, v_w_street_1, v_w_street_2, v_w_city, v_w_state, v_w_zip;
    
    -- Update District
    UPDATE district 
    SET d_ytd = d_ytd + p_h_amount
    WHERE d_w_id = p_w_id AND d_id = p_d_id
    RETURNING d_name, d_street_1, d_street_2, d_city, d_state, d_zip
    INTO v_d_name, v_d_street_1, v_d_street_2, v_d_city, v_d_state, v_d_zip;
    
    -- Build H_DATA
    v_h_data := v_w_name || '    ' || v_d_name;
    
    -- Customer selection logic
    IF p_c_id IS NOT NULL THEN
        -- Case 1: Select customer by ID
        SELECT c_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip,
               c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_data
        INTO v_c_id, v_c_first, v_c_middle, v_c_last, v_c_street_1, v_c_street_2, v_c_city,
             v_c_state, v_c_zip, v_c_phone, v_c_since, v_c_credit, v_c_credit_lim,
             v_c_discount, v_c_balance, v_c_data
        FROM customer
        WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_id = p_c_id;
        
    ELSE
        -- Case 2: Select customer by last name
        -- First count customers with the same last name
        SELECT COUNT(*) INTO v_customer_count
        FROM customer
        WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_last = p_c_last;
        
        -- Calculate middle index (n/2 rounded up)
        v_middle_customer_index := CEIL(v_customer_count::DECIMAL / 2);
        
        -- Get the middle customer using window function
        SELECT c_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip,
               c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_data
        INTO v_c_id, v_c_first, v_c_middle, v_c_last, v_c_street_1, v_c_street_2, v_c_city,
             v_c_state, v_c_zip, v_c_phone, v_c_since, v_c_credit, v_c_credit_lim,
             v_c_discount, v_c_balance, v_c_data
        FROM (
            SELECT c_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip,
                   c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_data,
                   ROW_NUMBER() OVER (ORDER BY c_first ASC) as row_num
            FROM customer
            WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_last = p_c_last
        ) AS sorted_customers
        WHERE row_num = v_middle_customer_index;
        
    END IF;
    
    -- Update customer balance and statistics
    UPDATE customer
    SET c_balance = c_balance - p_h_amount,
        c_ytd_payment = c_ytd_payment + p_h_amount,
        c_payment_cnt = c_payment_cnt + 1
    WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_id = v_c_id;
    
    -- Handle bad credit customers (C_CREDIT = 'BC')
    IF v_c_credit = 'BC' THEN
        -- Build new C_DATA with history information
        v_new_c_data := v_c_id || ' ' || p_c_d_id || ' ' || p_c_w_id || ' ' || 
                       p_d_id || ' ' || p_w_id || ' ' || p_h_amount || ' ' || 
                       v_history_timestamp || ' ' || COALESCE(v_c_data, '');
        
        -- Ensure C_DATA doesn't exceed 500 characters
        IF LENGTH(v_new_c_data) > 500 THEN
            v_new_c_data := SUBSTRING(v_new_c_data FROM 1 FOR 500);
        END IF;
        
        -- Update customer with new C_DATA
        UPDATE customer
        SET c_data = v_new_c_data
        WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_id = v_c_id;
        
        v_c_data := v_new_c_data;
    END IF;
    
    -- Insert into HISTORY table
    INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data)
    VALUES (v_c_id, p_c_d_id, p_c_w_id, p_d_id, p_w_id, v_history_timestamp, p_h_amount, v_h_data);
    
    -- Return all output data
    RETURN QUERY SELECT
        p_w_id, v_w_street_1, v_w_street_2, v_w_city, v_w_state, v_w_zip,
        p_d_id, v_d_street_1, v_d_street_2, v_d_city, v_d_state, v_d_zip,
        v_c_id, v_c_first, v_c_middle, v_c_last, v_c_street_1, v_c_street_2,
        v_c_city, v_c_state, v_c_zip, v_c_phone, v_c_since,
        v_c_credit, v_c_credit_lim, v_c_discount, (v_c_balance - p_h_amount) as new_balance,
        CASE WHEN v_c_credit = 'BC' THEN SUBSTRING(v_c_data FROM 1 FOR 200) ELSE NULL END as c_data_display,
        p_h_amount, v_history_timestamp;
    
END;
$$ LANGUAGE plpgsql;

--------------------
-- 1.2 - CHATGPT  --
--------------------
-- Payment transaction implementation for PostgreSQL (PL/pgSQL).
-- Adapt column/table names and types if your schema differs.

CREATE OR REPLACE FUNCTION tpcc_payment(
  p_w_id       integer,              -- originating/entering warehouse id (W_ID)
  p_d_id       integer,              -- district id where payment is entered (D_ID)
  p_c_w_id     integer,              -- customer's warehouse id (C_W_ID)
  p_c_d_id     integer,              -- customer's district id (C_D_ID)
  p_by_lastname boolean,             -- true -> select by last name, false -> by c_id
  p_c_id       integer DEFAULT NULL, -- required when p_by_lastname = false
  p_c_last     text    DEFAULT NULL, -- required when p_by_lastname = true
  p_h_amount   numeric(12,2),        -- payment amount H_AMOUNT
  p_h_date     timestamptz DEFAULT now() -- H_DATE (defaults to now())
)
RETURNS TABLE (
  out_w_id       integer,
  out_d_id       integer,
  out_c_id       integer,
  out_c_w_id     integer,
  out_c_d_id     integer,
  -- Warehouse address and name fields
  w_name         text,
  w_street_1     text,
  w_street_2     text,
  w_city         text,
  w_state        text,
  w_zip          text,
  -- District address and name fields
  d_name         text,
  d_street_1     text,
  d_street_2     text,
  d_city         text,
  d_state        text,
  d_zip          text,
  -- Customer fields required by spec
  c_first        text,
  c_middle       text,
  c_last         text,
  c_street_1     text,
  c_street_2     text,
  c_city         text,
  c_state        text,
  c_zip          text,
  c_phone        text,
  c_since        timestamptz,
  c_credit       text,
  c_credit_lim   numeric(12,2),
  c_discount     numeric(6,4),
  c_balance      numeric(12,2),
  c_data_display text,  -- first 200 chars of c_data if c_credit = 'BC', otherwise NULL
  h_amount       numeric(12,2),
  h_date         timestamptz
) AS $$
DECLARE
  v_w_ytd numeric(18,2);
  v_d_ytd numeric(18,2);

  v_selected_c_id integer;
  v_c_data text;
  v_hist_text text;
  v_c_record record;

  v_ids integer[]; -- used when selecting by last name
  v_n integer;
  v_idx integer;
BEGIN
  -- 1) Warehouse: select and lock
  SELECT w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_ytd
    INTO w_name, w_street_1, w_street_2, w_city, w_state, w_zip, v_w_ytd
  FROM warehouse
  WHERE w_id = p_w_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'WAREHOUSE % not found', p_w_id;
  END IF;

  -- update warehouse YTD
  UPDATE warehouse
    SET w_ytd = w_ytd + p_h_amount
  WHERE w_id = p_w_id;

  -- 2) District: select and lock
  SELECT d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_ytd
    INTO d_name, d_street_1, d_street_2, d_city, d_state, d_zip, v_d_ytd
  FROM district
  WHERE d_w_id = p_w_id AND d_id = p_d_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'DISTRICT %/% not found', p_w_id, p_d_id;
  END IF;

  -- update district YTD
  UPDATE district
    SET d_ytd = d_ytd + p_h_amount
  WHERE d_w_id = p_w_id AND d_id = p_d_id;

  -- 3) Customer: selection logic
  IF p_by_lastname THEN
    IF p_c_last IS NULL THEN
      RAISE EXCEPTION 'p_c_last is required when p_by_lastname = true';
    END IF;

    -- select c_id list ordered by c_first (ascending)
    SELECT array_agg(c_id ORDER BY c_first)
      INTO v_ids
    FROM customer
    WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_last = p_c_last;

    IF v_ids IS NULL OR array_length(v_ids,1) = 0 THEN
      RAISE EXCEPTION 'No customer with last name % in %/%', p_c_last, p_c_w_id, p_c_d_id;
    END IF;

    v_n := array_length(v_ids,1);
    v_idx := (v_n + 1) / 2; -- middle (n/2 rounded up)
    v_selected_c_id := v_ids[v_idx];

    -- fetch and lock the selected customer row
    SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip,
           c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_data
      INTO c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip,
           c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, -- c_ytd_payment/c_payment_cnt not returned
           , v_c_data
    FROM customer
    WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_id = v_selected_c_id
    FOR UPDATE;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'Selected customer id % not found for %/%', v_selected_c_id, p_c_w_id, p_c_d_id;
    END IF;

  ELSE
    -- by customer id mode
    IF p_c_id IS NULL THEN
      RAISE EXCEPTION 'p_c_id is required when p_by_lastname = false';
    END IF;
    v_selected_c_id := p_c_id;

    SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip,
           c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_data
      INTO c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip,
           c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, -- keep c_ytd_payment & c_payment_cnt locally if needed
           , v_c_data
    FROM customer
    WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_id = v_selected_c_id
    FOR UPDATE;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'Customer %/%/% not found', p_c_w_id, p_c_d_id, v_selected_c_id;
    END IF;
  END IF;

  -- 4) Update customer balances and counters
  -- decrease c_balance by h_amount; increase c_ytd_payment; increment c_payment_cnt
  UPDATE customer
    SET c_balance = c_balance - p_h_amount,
        c_ytd_payment = c_ytd_payment + p_h_amount,
        c_payment_cnt = c_payment_cnt + 1
  WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_id = v_selected_c_id;

  -- 5) If c_credit = 'BC' then prepend history info to c_data (shift right) and truncate to 500 chars
  IF c_credit = 'BC' THEN
    -- create history string formatted: C_ID, C_D_ID, C_W_ID, D_ID, W_ID, H_AMOUNT (space-separated)
    v_hist_text := format('%s %s %s %s %s %s | ',
                          v_selected_c_id, p_c_d_id, p_c_w_id, p_d_id, p_w_id, p_h_amount::text);

    -- new c_data is leftmost 500 characters of (hist_text || old c_data)
    v_c_data := substr(v_hist_text || coalesce(v_c_data, ''), 1, 500);

    UPDATE customer
      SET c_data = v_c_data
    WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_id = v_selected_c_id;
  END IF;

  -- 6) Insert into history table
  -- H_DATA = W_NAME || 4 spaces || D_NAME
  INSERT INTO history (
    h_c_id, h_c_d_id, h_c_w_id,
    h_d_id, h_w_id, h_date, h_amount, h_data
  ) VALUES (
    v_selected_c_id, p_c_d_id, p_c_w_id,
    p_d_id, p_w_id, p_h_date, p_h_amount,
    (w_name || '    ' || d_name)
  );

  -- 7) Prepare outputs; re-query customer (to get updated balance, etc.) and return the output record.
  SELECT c_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip,
         c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_data
    INTO out_c_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip,
         c_phone, c_since, c_credit, c_credit_lim, c_discount, out_c_balance, v_c_data
  FROM customer
  WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_id = v_selected_c_id;

  -- Final assignment to output fields:
  out_w_id := p_w_id;
  out_d_id := p_d_id;
  out_c_id := v_selected_c_id;
  out_c_w_id := p_c_w_id;
  out_c_d_id := p_c_d_id;

  -- w_*, d_* and customer fields already loaded into named return columns; set remaining return columns:
  h_amount := p_h_amount;
  h_date := p_h_date;

  -- c_data_display is first 200 chars only if credit = 'BC'
  IF c_credit = 'BC' THEN
    c_data_display := substr(v_c_data, 1, 200);
  ELSE
    c_data_display := NULL;
  END IF;

  c_first := c_first;
  c_middle := c_middle;
  c_last := c_last;
  c_street_1 := c_street_1;
  c_street_2 := c_street_2;
  c_city := c_city;
  c_state := c_state;
  c_zip := c_zip;
  c_phone := c_phone;
  c_since := c_since;
  c_credit := c_credit;
  c_credit_lim := c_credit_lim;
  c_discount := c_discount;
  c_balance := out_c_balance;

  RETURN NEXT;
  RETURN;
END;
$$ LANGUAGE plpgsql;


-------------------
-- 1.3 - GEMINI  --
-------------------
-- Assuming the following TPC-C table structure is in place:
-- WAREHOUSE (W_ID, W_YTD, ...)
-- DISTRICT (D_W_ID, D_ID, D_YTD, ...)
-- CUSTOMER (C_W_ID, C_D_ID, C_ID, C_LAST, C_FIRST, C_MIDDLE, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_CREDIT, C_DATA, ...)
-- HISTORY (H_C_ID, H_C_D_ID, H_C_W_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA)

CREATE OR REPLACE PROCEDURE payment_transaction(
    IN w_id_in INT,
    IN d_id_in INT,
    IN c_w_id_in INT,
    IN c_d_id_in INT,
    IN c_id_in INT,        -- Used if selection is by ID
    IN c_last_in VARCHAR,  -- Used if selection is by LAST NAME
    IN h_amount_in DECIMAL
)
LANGUAGE plpgsql AS $$
DECLARE
    -- WAREHOUSE variables
    w_name_var VARCHAR(10);
    w_street_1_var VARCHAR(20);
    w_city_var VARCHAR(20);

    -- DISTRICT variables
    d_name_var VARCHAR(10);
    d_street_1_var VARCHAR(20);
    d_city_var VARCHAR(20);

    -- CUSTOMER variables
    c_id_var INT;
    c_first_var VARCHAR(16);
    c_middle_var CHAR(2);
    c_last_var VARCHAR(16);
    c_credit_var CHAR(2);
    c_balance_var DECIMAL(12,2);
    c_data_var VARCHAR(500);

    -- Other variables
    h_data_var CHAR(24);
    customer_count INT;
BEGIN
    -- 1. Update WAREHOUSE (W_ID is the 'home' warehouse for the terminal)
    UPDATE WAREHOUSE
    SET W_YTD = W_YTD + h_amount_in
    WHERE W_ID = w_id_in
    RETURNING W_NAME, W_STREET_1, W_CITY INTO w_name_var, w_street_1_var, w_city_var;

    -- 2. Update DISTRICT (D_W_ID is the home warehouse)
    UPDATE DISTRICT
    SET D_YTD = D_YTD + h_amount_in
    WHERE D_W_ID = w_id_in AND D_ID = d_id_in
    RETURNING D_NAME, D_STREET_1, D_CITY INTO d_name_var, d_street_1_var, d_city_var;

    -- 3. Select and Update CUSTOMER
    IF c_id_in IS NOT NULL THEN
        -- Case 1: Customer selected by number (C_ID)
        SELECT
            C_FIRST, C_MIDDLE, C_LAST, C_CREDIT, C_BALANCE, C_DATA, C_ID
        INTO
            c_first_var, c_middle_var, c_last_var, c_credit_var, c_balance_var, c_data_var, c_id_var
        FROM CUSTOMER
        WHERE C_W_ID = c_w_id_in AND C_D_ID = c_d_id_in AND C_ID = c_id_in
        FOR UPDATE; -- Lock the row

        c_id_var := c_id_in; -- Redundant, but ensures C_ID is set for HISTORY insertion

    ELSE
        -- Case 2: Customer selected by last name (C_LAST)
        -- The TPC-C spec requires selecting the middle row of *all* customers 
        -- matching C_W_ID, C_D_ID, and C_LAST, sorted by C_FIRST.

        -- A common TPC-C implementation technique to select the 'middle' of a small set
        -- is using window functions (or a simpler approach with COUNT and OFFSET/LIMIT).

        -- First, find the count of customers with the matching last name
        SELECT COUNT(*) 
        INTO customer_count 
        FROM CUSTOMER
        WHERE C_W_ID = c_w_id_in AND C_D_ID = c_d_id_in AND C_LAST = c_last_in;

        -- Select the middle row: (n/2 rounded up)
        SELECT
            C_ID, C_FIRST, C_MIDDLE, C_LAST, C_CREDIT, C_BALANCE, C_DATA
        INTO
            c_id_var, c_first_var, c_middle_var, c_last_var, c_credit_var, c_balance_var, c_data_var
        FROM CUSTOMER
        WHERE C_W_ID = c_w_id_in AND C_D_ID = c_d_id_in AND C_LAST = c_last_in
        ORDER BY C_FIRST
        OFFSET (customer_count + 1) / 2 - 1 ROWS FETCH FIRST 1 ROW ONLY
        FOR UPDATE; -- Lock the row
    END IF;

    -- Update Customer's Balance and Payment Counts
    UPDATE CUSTOMER
    SET
        C_BALANCE = C_BALANCE - h_amount_in,
        C_YTD_PAYMENT = C_YTD_PAYMENT + h_amount_in,
        C_PAYMENT_CNT = C_PAYMENT_CNT + 1
    WHERE C_W_ID = c_w_id_in AND C_D_ID = c_d_id_in AND C_ID = c_id_var;
    
    -- 4. Handle "Bad Credit" (BC) customers
    IF c_credit_var = 'BC' THEN
        -- Construct the history data to prepend (max 500 chars total for C_DATA)
        -- Format: C_ID, C_D_ID, C_W_ID, D_ID, W_ID, H_AMOUNT, and current C_DATA
        c_data_var := format('%s %s %s %s %s $%s %s',
                             c_id_var, c_d_id_in, c_w_id_in, d_id_in, w_id_in, h_amount_in, c_data_var);
        
        -- Truncate C_DATA to 500 characters
        c_data_var := left(c_data_var, 500);

        -- Update C_DATA for the 'BC' customer
        UPDATE CUSTOMER
        SET C_DATA = c_data_var
        WHERE C_W_ID = c_w_id_in AND C_D_ID = c_d_id_in AND C_ID = c_id_var;
    END IF;

    -- 5. Insert into HISTORY table
    -- H_DATA is W_NAME concatenated with D_NAME, separated by 4 spaces
    h_data_var := w_name_var || '    ' || d_name_var;

    INSERT INTO HISTORY (
        H_C_ID, H_C_D_ID, H_C_W_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA
    ) VALUES (
        c_id_var, c_d_id_in, c_w_id_in, d_id_in, w_id_in, NOW(), h_amount_in, h_data_var
    );

    -- All statements are implicitly committed upon successful completion of the PROCEDURE, 
    -- as it runs within an implicit transaction block (unless an explicit BEGIN is used).

END;
$$;