-- Results for payment transation ** Basic Text-to-SQL

---------------------
-- 1.1 - DEEPSEEK  --
---------------------
CREATE OR REPLACE FUNCTION bmsql_proc_payment(
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
    UPDATE bmsql_warehouse 
    SET w_ytd = w_ytd + p_h_amount
    WHERE w_id = p_w_id
    RETURNING w_name, w_street_1, w_street_2, w_city, w_state, w_zip
    INTO v_w_name, v_w_street_1, v_w_street_2, v_w_city, v_w_state, v_w_zip;
    
    -- Update District
    UPDATE bmsql_district 
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
        FROM bmsql_customer
        WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_id = p_c_id;
        
    ELSE
        -- Case 2: Select customer by last name
        -- First count customers with the same last name
        SELECT COUNT(*) INTO v_customer_count
        FROM bmsql_customer
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
            FROM bmsql_customer
            WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_last = p_c_last
        ) AS sorted_customers
        WHERE row_num = v_middle_customer_index;
        
    END IF;
    
    -- Update customer balance and statistics
    UPDATE bmsql_customer
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
        UPDATE bmsql_customer
        SET c_data = v_new_c_data
        WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_id = v_c_id;
        
        v_c_data := v_new_c_data;
    END IF;
    
    -- Insert into HISTORY table
    INSERT INTO bmsql_history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data)
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
CREATE OR REPLACE FUNCTION bmsql_proc_payment(
    p_w_id       integer,
    p_d_id       integer,
    p_c_w_id     integer,
    p_c_d_id     integer,
    p_by_lastname boolean,
    p_c_id       integer DEFAULT NULL,
    p_c_last     text    DEFAULT NULL,
    p_h_amount   numeric(12,2),
    p_h_date     timestamptz DEFAULT now()
)
RETURNS TABLE (
    out_w_id       integer,
    out_d_id       integer,
    out_c_id       integer,
    out_c_w_id     integer,
    out_c_d_id     integer,
    w_name         text,
    w_street_1     text,
    w_street_2     text,
    w_city         text,
    w_state        text,
    w_zip          text,
    d_name         text,
    d_street_1     text,
    d_street_2     text,
    d_city         text,
    d_state        text,
    d_zip          text,
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
    c_data_display text,
    h_amount       numeric(12,2),
    h_date         timestamptz
) AS $$
DECLARE
    v_w_ytd numeric(18,2);
    v_d_ytd numeric(18,2);
    v_selected_c_id integer;
    v_c_data text;
    v_hist_text text;
    v_ids integer[];
    v_n integer;
    v_idx integer;
BEGIN
    ----------------------------------------------------------------------
    -- 1) Warehouse
    ----------------------------------------------------------------------
    SELECT w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_ytd
    INTO w_name, w_street_1, w_street_2, w_city, w_state, w_zip, v_w_ytd
    FROM warehouse
    WHERE w_id = p_w_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'WAREHOUSE % not found', p_w_id;
    END IF;

    UPDATE warehouse
    SET w_ytd = w_ytd + p_h_amount
    WHERE w_id = p_w_id;

    ----------------------------------------------------------------------
    -- 2) District
    ----------------------------------------------------------------------
    SELECT d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_ytd
    INTO d_name, d_street_1, d_street_2, d_city, d_state, d_zip, v_d_ytd
    FROM district
    WHERE d_w_id = p_w_id AND d_id = p_d_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'DISTRICT %/% not found', p_w_id, p_d_id;
    END IF;

    UPDATE district
    SET d_ytd = d_ytd + p_h_amount
    WHERE d_w_id = p_w_id AND d_id = p_d_id;

    ----------------------------------------------------------------------
    -- 3) Customer selection
    ----------------------------------------------------------------------
    IF p_by_lastname THEN
        IF p_c_last IS NULL THEN
            RAISE EXCEPTION 'p_c_last is required when selecting by last name';
        END IF;

        -- list of matching customers ordered by first name
        SELECT array_agg(c_id ORDER BY c_first)
        INTO v_ids
        FROM customer
        WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_last = p_c_last;

        IF v_ids IS NULL OR array_length(v_ids,1) = 0 THEN
            RAISE EXCEPTION 'No customers with last name % in %/%',
                p_c_last, p_c_w_id, p_c_d_id;
        END IF;

        v_n := array_length(v_ids,1);
        v_idx := (v_n + 1) / 2;
        v_selected_c_id := v_ids[v_idx];
    ELSE
        IF p_c_id IS NULL THEN
            RAISE EXCEPTION 'p_c_id required when selecting by id';
        END IF;

        v_selected_c_id := p_c_id;
    END IF;

    ----------------------------------------------------------------------
    -- Fetch customer for update
    ----------------------------------------------------------------------
    SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city,
           c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim,
           c_discount, c_balance, c_data
    INTO c_first, c_middle, c_last, c_street_1, c_street_2, c_city,
         c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim,
         c_discount, c_balance, v_c_data
    FROM customer
    WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_id = v_selected_c_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Customer %/%/% not found',
            p_c_w_id, p_c_d_id, v_selected_c_id;
    END IF;

    ----------------------------------------------------------------------
    -- 4) Update customer balance
    ----------------------------------------------------------------------
    UPDATE customer
    SET c_balance = c_balance - p_h_amount,
        c_ytd_payment = c_ytd_payment + p_h_amount,
        c_payment_cnt = c_payment_cnt + 1
    WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_id = v_selected_c_id;

    ----------------------------------------------------------------------
    -- 5) Update c_data if credit='BC'
    ----------------------------------------------------------------------
    IF c_credit = 'BC' THEN
        v_hist_text := format('%s %s %s %s %s %s | ',
                              v_selected_c_id, p_c_d_id, p_c_w_id,
                              p_d_id, p_w_id, p_h_amount);

        v_c_data := substr(v_hist_text || coalesce(v_c_data, ''), 1, 500);

        UPDATE customer
        SET c_data = v_c_data
        WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_id = v_selected_c_id;
    END IF;

    ----------------------------------------------------------------------
    -- 6) Insert history row
    ----------------------------------------------------------------------
    INSERT INTO history (
        h_c_id, h_c_d_id, h_c_w_id,
        h_d_id, h_w_id, h_date, h_amount, h_data
    ) VALUES (
        v_selected_c_id, p_c_d_id, p_c_w_id,
        p_d_id, p_w_id, p_h_date, p_h_amount,
        (w_name || '    ' || d_name)
    );

    ----------------------------------------------------------------------
    -- 7) Final customer read for output
    ----------------------------------------------------------------------
    SELECT c_id, c_first, c_middle, c_last, c_street_1, c_street_2,
           c_city, c_state, c_zip, c_phone, c_since, c_credit,
           c_credit_lim, c_discount, c_balance
    INTO out_c_id, c_first, c_middle, c_last, c_street_1, c_street_2,
         c_city, c_state, c_zip, c_phone, c_since, c_credit,
         c_credit_lim, c_discount, c_balance
    FROM customer
    WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_id = v_selected_c_id;

    out_w_id := p_w_id;
    out_d_id := p_d_id;
    out_c_w_id := p_c_w_id;
    out_c_d_id := p_c_d_id;

    c_data_display :=
        CASE WHEN c_credit = 'BC'
             THEN substr(v_c_data, 1, 200)
             ELSE NULL
        END;

    h_amount := p_h_amount;
    h_date   := p_h_date;

    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;



-------------------
-- 1.3 - GEMINI  --
-------------------
-- Use CREATE FUNCTION for broader compatibility and easy execution via SELECT
CREATE OR REPLACE FUNCTION bmsql_proc_payment(
    IN w_id_in INT,
    IN d_id_in INT,
    IN c_w_id_in INT,
    IN c_d_id_in INT,
    IN c_id_in INT,        -- Used if selection is by ID
    IN c_last_in VARCHAR,  -- Used if selection is by LAST NAME
    IN h_amount_in DECIMAL
)
RETURNS VOID -- A procedure/function that performs updates but returns nothing
LANGUAGE plpgsql AS $BODY$
DECLARE
    -- WAREHOUSE variables (You should also retrieve W_STREET_2, W_STATE, W_ZIP, etc., as per TPC-C spec)
    w_name_var VARCHAR(10);
    -- ... other WAREHOUSE fields ...

    -- DISTRICT variables
    d_name_var VARCHAR(10);
    -- ... other DISTRICT fields ...

    -- CUSTOMER variables
    c_id_var INT;
    c_credit_var CHAR(2);
    c_data_var VARCHAR(500);
    customer_count INT;
    
    -- Added H_DATA variable
    h_data_var CHAR(24); 
BEGIN
    -- 1. Update WAREHOUSE and retrieve required fields (Using W_ID as home warehouse)
    UPDATE WAREHOUSE
    SET W_YTD = W_YTD + h_amount_in
    WHERE W_ID = w_id_in
    RETURNING W_NAME INTO w_name_var; -- Placeholder, should return all W_ fields

    -- 2. Update DISTRICT and retrieve required fields
    UPDATE DISTRICT
    SET D_YTD = D_YTD + h_amount_in
    WHERE D_W_ID = w_id_in AND D_ID = d_id_in
    RETURNING D_NAME INTO d_name_var; -- Placeholder, should return all D_ fields

    -- 3. Select and Update CUSTOMER
    IF c_id_in IS NOT NULL THEN
        -- Case 1: Customer selected by number (C_ID)
        SELECT
            C_CREDIT, C_DATA, C_ID -- Retrieve other C_ fields as per spec
        INTO
            c_credit_var, c_data_var, c_id_var
        FROM CUSTOMER
        WHERE C_W_ID = c_w_id_in AND C_D_ID = c_d_id_in AND C_ID = c_id_in
        FOR UPDATE; -- Lock the row

        c_id_var := c_id_in; 

    ELSE
        -- Case 2: Customer selected by last name (C_LAST)
        SELECT COUNT(*) INTO customer_count 
        FROM CUSTOMER
        WHERE C_W_ID = c_w_id_in AND C_D_ID = c_d_id_in AND C_LAST = c_last_in;

        -- Select the middle row (n/2 rounded up)
        SELECT
            C_ID, C_CREDIT, C_DATA -- Retrieve other C_ fields as per spec
        INTO
            c_id_var, c_credit_var, c_data_var
        FROM CUSTOMER
        WHERE C_W_ID = c_w_id_in AND C_D_ID = c_d_id_in AND C_LAST = c_last_in
        ORDER BY C_FIRST
        OFFSET ((customer_count + 1) / 2 - 1) ROWS FETCH FIRST 1 ROW ONLY
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
        -- Re-build history string (Requires D_ID and W_ID to be passed or accessible)
        h_data_var := format('%s %s %s %s %s $%s',
                             c_id_var, c_d_id_in, c_w_id_in, d_id_in, w_id_in, h_amount_in);
        
        -- Prepend and Truncate C_DATA to 500 characters
        c_data_var := left(h_data_var || ' ' || c_data_var, 500);

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
END;
$BODY$;