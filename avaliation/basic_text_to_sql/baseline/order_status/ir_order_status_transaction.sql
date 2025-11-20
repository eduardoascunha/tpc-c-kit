-- Results for order status transation ** Basic Text-to-SQL

---------------------
-- 1.1 - DEEPSEEK  --
---------------------
-- Order-Status Transaction for TPC-C
CREATE OR REPLACE FUNCTION bmsql_proc_order_status(
    p_w_id INTEGER,
    p_d_id INTEGER,
    p_c_id INTEGER DEFAULT NULL,
    p_c_last VARCHAR DEFAULT NULL
) RETURNS TABLE(
    c_id INTEGER,
    c_first VARCHAR,
    c_middle VARCHAR,
    c_last VARCHAR,
    c_balance NUMERIC,
    o_id INTEGER,
    o_entry_d TIMESTAMP,
    o_carrier_id INTEGER,
    ol_supply_w_id INTEGER,
    ol_i_id INTEGER,
    ol_quantity NUMERIC,
    ol_amount NUMERIC,
    ol_delivery_d TIMESTAMP
) AS $$
DECLARE
    v_customer_rec RECORD;
    v_order_rec RECORD;
    v_customer_count INTEGER;
    v_customer_position INTEGER;
BEGIN
    -- Validate that either customer ID or last name is provided
    IF p_c_id IS NULL AND p_c_last IS NULL THEN
        RAISE EXCEPTION 'Either customer ID or last name must be provided';
    END IF;

    -- Case 1: Customer selected by customer number
    IF p_c_id IS NOT NULL THEN
        -- Get customer by ID
        SELECT c_id, c_first, c_middle, c_last, c_balance
        INTO v_customer_rec
        FROM bmsql_customer
        WHERE c_w_id = p_w_id 
          AND c_d_id = p_d_id 
          AND c_id = p_c_id;
        
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Customer not found with ID: %', p_c_id;
        END IF;

    -- Case 2: Customer selected by last name  
    ELSE
        -- Count customers with the given last name
        SELECT COUNT(*)
        INTO v_customer_count
        FROM bmsql_customer
        WHERE c_w_id = p_w_id 
          AND c_d_id = p_d_id 
          AND c_last = p_c_last;
        
        IF v_customer_count = 0 THEN
            RAISE EXCEPTION 'No customers found with last name: %', p_c_last;
        END IF;

        -- Calculate position (n/2 rounded up)
        v_customer_position := CEIL(v_customer_count::NUMERIC / 2);
        
        -- Get customer at the calculated position
        SELECT c_id, c_first, c_middle, c_last, c_balance
        INTO v_customer_rec
        FROM (
            SELECT c_id, c_first, c_middle, c_last, c_balance,
                   ROW_NUMBER() OVER (ORDER BY c_first ASC) as row_num
            FROM bmsql_customer
            WHERE c_w_id = p_w_id 
              AND c_d_id = p_d_id 
              AND c_last = p_c_last
        ) AS sorted_customers
        WHERE row_num = v_customer_position;
        
    END IF;

    -- Get the most recent order for the customer
    SELECT o_id, o_entry_d, o_carrier_id
    INTO v_order_rec
    FROM bmsql_oorder
    WHERE o_w_id = p_w_id 
      AND o_d_id = p_d_id 
      AND o_c_id = v_customer_rec.c_id
    ORDER BY o_id DESC
    LIMIT 1;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'No orders found for customer: %', v_customer_rec.c_id;
    END IF;

    -- Return customer and order information along with order lines
    RETURN QUERY
    SELECT 
        v_customer_rec.c_id,
        v_customer_rec.c_first,
        v_customer_rec.c_middle, 
        v_customer_rec.c_last,
        v_customer_rec.c_balance,
        v_order_rec.o_id,
        v_order_rec.o_entry_d,
        v_order_rec.o_carrier_id,
        ol.ol_supply_w_id,
        ol.ol_i_id,
        ol.ol_quantity,
        ol.ol_amount,
        ol.ol_delivery_d
    FROM bmsql_order_line ol
    WHERE ol.ol_w_id = p_w_id 
      AND ol.ol_d_id = p_d_id 
      AND ol.ol_o_id = v_order_rec.o_id
    ORDER BY ol.ol_number;

END;
$$ LANGUAGE plpgsql;


--------------------
-- 1.2 - CHATGPT  --
--------------------
CREATE OR REPLACE FUNCTION bmsql_proc_order_status(
    p_w_id       integer,
    p_d_id       integer,
    p_by_last    boolean,         -- true => lookup by last name, false => lookup by customer number
    p_c_id       integer DEFAULT NULL,
    p_c_last     text    DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
AS $func$
DECLARE
    v_customer  RECORD;
    v_cnt       integer;
    v_offset    integer;
    v_order     RECORD;
    v_lines     jsonb;
BEGIN
    ----------------------------------------------------------------------
    -- Validate input
    ----------------------------------------------------------------------
    IF p_by_last THEN
        IF p_c_last IS NULL THEN
            RAISE EXCEPTION 'p_c_last must be provided when p_by_last = true';
        END IF;
    ELSE
        IF p_c_id IS NULL THEN
            RAISE EXCEPTION 'p_c_id must be provided when p_by_last = false';
        END IF;
    END IF;

    ----------------------------------------------------------------------
    -- 1) Find the customer
    ----------------------------------------------------------------------
    IF NOT p_by_last THEN
        -- Case 1: lookup by customer number
        SELECT c_id, c_first, c_middle, c_last, c_balance
        INTO v_customer
        FROM customer
        WHERE c_w_id = p_w_id
          AND c_d_id = p_d_id
          AND c_id   = p_c_id
        LIMIT 1;

        IF NOT FOUND THEN
            RAISE EXCEPTION 'Customer not found: W=% D=% C=%', p_w_id, p_d_id, p_c_id;
        END IF;

    ELSE
        -- Case 2: lookup by last name (possibly multiple rows)
        SELECT count(*)
        INTO v_cnt
        FROM customer
        WHERE c_w_id = p_w_id
          AND c_d_id = p_d_id
          AND c_last = p_c_last;

        IF v_cnt = 0 THEN
            RAISE EXCEPTION 'Customer last name not found: W=% D=% LAST=%',
                p_w_id, p_d_id, p_c_last;
        END IF;

        -- Middle position = ceil(n/2) = (n - 1)/2 offset
        v_offset := (v_cnt - 1) / 2;

        SELECT c_id, c_first, c_middle, c_last, c_balance
        INTO v_customer
        FROM customer
        WHERE c_w_id = p_w_id
          AND c_d_id = p_d_id
          AND c_last = p_c_last
        ORDER BY c_first ASC
        OFFSET v_offset
        LIMIT 1;
    END IF;

    ----------------------------------------------------------------------
    -- 2) Get last order for that customer
    ----------------------------------------------------------------------
    SELECT o_id, o_entry_d, o_carrier_id, o_w_id, o_d_id
    INTO v_order
    FROM "order"
    WHERE o_w_id = p_w_id
      AND o_d_id = p_d_id
      AND o_c_id = v_customer.c_id
    ORDER BY o_id DESC
    LIMIT 1;

    IF NOT FOUND THEN
        -- Customer has no orders
        RETURN jsonb_build_object(
            'W_ID', p_w_id,
            'D_ID', p_d_id,
            'C_ID', v_customer.c_id,
            'C_FIRST', v_customer.c_first,
            'C_MIDDLE', v_customer.c_middle,
            'C_LAST', v_customer.c_last,
            'C_BALANCE', v_customer.c_balance,
            'ORDER', NULL,
            'ORDER_LINES', jsonb '[]'
        );
    END IF;

    ----------------------------------------------------------------------
    -- 3) Collect order lines
    ----------------------------------------------------------------------
    SELECT jsonb_agg(
        jsonb_build_object(
            'OL_SUPPLY_W_ID', ol_supply_w_id,
            'OL_I_ID',        ol_i_id,
            'OL_QUANTITY',    ol_quantity,
            'OL_AMOUNT',      ol_amount,
            'OL_DELIVERY_D',  ol_delivery_d
        )
        ORDER BY ol_number
    )
    INTO v_lines
    FROM order_line
    WHERE ol_w_id = v_order.o_w_id
      AND ol_d_id = v_order.o_d_id
      AND ol_o_id = v_order.o_id;

    IF v_lines IS NULL THEN
        v_lines := '[]'::jsonb;
    END IF;

    ----------------------------------------------------------------------
    -- 4) Return JSON summary
    ----------------------------------------------------------------------
    RETURN jsonb_build_object(
        'W_ID', p_w_id,
        'D_ID', p_d_id,
        'C_ID', v_customer.c_id,
        'C_FIRST', v_customer.c_first,
        'C_MIDDLE', v_customer.c_middle,
        'C_LAST', v_customer.c_last,
        'C_BALANCE', v_customer.c_balance,
        'O_ID', v_order.o_id,
        'O_ENTRY_D', v_order.o_entry_d,
        'O_CARRIER_ID', v_order.o_carrier_id,
        'ORDER_LINES', v_lines
    );
END;
$func$;



-------------------
-- 1.3 - GEMINI  --
-------------------
CREATE OR REPLACE FUNCTION bmsql_proc_order_status(
    IN p_w_id INT,
    IN p_d_id INT,
    IN p_c_id INT DEFAULT NULL,
    IN p_c_last VARCHAR DEFAULT NULL,
    OUT out_c_id INT,
    OUT out_c_first VARCHAR,
    OUT out_c_middle VARCHAR,
    OUT out_c_last VARCHAR,
    OUT out_c_balance NUMERIC,
    OUT out_o_id INT,
    OUT out_o_entry_d TIMESTAMP,
    OUT out_o_carrier_id INT,
    OUT out_order_lines JSONB
)
LANGUAGE plpgsql AS $$
#variable_conflict use_column
DECLARE
    v_c_id INT;
BEGIN
    -- Step 1: Find the Customer
    -------------------------------------------------------
    IF p_c_id IS NOT NULL THEN
        -- Case 1: Customer selected by C_ID
        SELECT
            c_id, c_first, c_middle, c_last, c_balance
        INTO
            v_c_id, out_c_first, out_c_middle, out_c_last, out_c_balance
        FROM
            CUSTOMER
        WHERE
            c_w_id = p_w_id AND c_d_id = p_d_id AND c_id = p_c_id;
        
        out_c_id := v_c_id;

    ELSIF p_c_last IS NOT NULL THEN
        -- Case 2: Customer selected by C_LAST (Middle customer logic: n/2 rounded up)
        WITH SelectedCustomers AS (
            SELECT
                c_id, c_first, c_middle, c_last, c_balance,
                ROW_NUMBER() OVER (ORDER BY c_first ASC) AS rn,
                COUNT(*) OVER () AS total_count
            FROM
                CUSTOMER
            WHERE
                c_w_id = p_w_id AND c_d_id = p_d_id AND c_last = p_c_last
        )
        SELECT
            c_id, c_first, c_middle, c_last, c_balance
        INTO
            v_c_id, out_c_first, out_c_middle, out_c_last, out_c_balance
        FROM
            SelectedCustomers
        WHERE
            rn = CEIL(total_count / 2.0);
            
        out_c_id := v_c_id;

    ELSE
        -- Error handling for missing input
        RAISE EXCEPTION 'Must provide either p_c_id or p_c_last.';
    END IF;

    IF v_c_id IS NULL THEN
        RAISE EXCEPTION 'Customer not found (W=% D=% C=% L=%)', p_w_id, p_d_id, p_c_id, p_c_last;
    END IF;

    -- Step 2: Find the Customer's Last Order
    -------------------------------------------------------
    SELECT
        o_id, o_entry_d, o_carrier_id
    INTO
        out_o_id, out_o_entry_d, out_o_carrier_id
    FROM
        "ORDER" -- Quotes needed if "ORDER" is a reserved keyword
    WHERE
        o_w_id = p_w_id
        AND o_d_id = p_d_id
        AND o_c_id = v_c_id
    ORDER BY
        o_id DESC
    LIMIT 1;
    
    IF out_o_id IS NULL THEN
        -- No orders found for customer
        out_order_lines := '[]'::JSONB;
        RETURN;
    END IF;

    -- Step 3: Retrieve all Order-Lines for the Last Order
    -------------------------------------------------------
    SELECT
        jsonb_agg(jsonb_build_object(
            'ol_i_id', ol_i_id,
            'ol_supply_w_id', ol_supply_w_id,
            'ol_quantity', ol_quantity,
            'ol_amount', ol_amount,
            'ol_delivery_d', ol_delivery_d
        ))
    INTO
        out_order_lines
    FROM
        ORDER_LINE
    WHERE
        ol_w_id = p_w_id
        AND ol_d_id = p_d_id
        AND ol_o_id = out_o_id
    ORDER BY
        ol_i_id ASC;
        
END;
$$;
