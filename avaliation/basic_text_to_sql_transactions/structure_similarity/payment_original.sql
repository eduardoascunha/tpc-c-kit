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