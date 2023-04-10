DDL_QUERY_DW = '''

--
-- Name: film; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE dimFilm (
    film_id integer NOT NULL PRIMARY KEY,
    title character varying(255) NOT NULL,
    actor character varying(255) NOT NULL,
    category character varying(25) NOT NULL,
    release_year integer NOT NULL,
    language character(20) NOT NULL,
    length smallint,
    replacement_cost numeric(5,2) NOT NULL,
    rating character(20) NOT NULL
);
ALTER TABLE dimFilm OWNER TO postgres;


--
-- Name: dimStore; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE dimStore (
    staff_id integer NOT NULL PRIMARY KEY,
    staff_name character varying(100) NOT NULL,
    store_id smallint NOT NULL,
    store_address character varying(50) NOT NULL,
    store_district character varying(50) NOT NULL,
    store_city character varying(50) NOT NULL,
    store_country character varying(50) NOT NULL
);
ALTER TABLE dimStore OWNER TO postgres;



--
-- Name: dimCustomer; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE dimCustomer (
    customer_id integer NOT NULL PRIMARY KEY,
    customer_name character varying(100) NOT NULL,
    email character varying(50),
    customer_address character varying(50) NOT NULL,
    customer_district character varying(50) NOT NULL,
    customer_city character varying(50) NOT NULL,
    customer_country character varying(50) NOT NULL,
    activebool boolean DEFAULT true NOT NULL
);
ALTER TABLE dimCustomer OWNER TO postgres;


--
-- Name: dimDate; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE dimDate (
    date_id character varying(30) NOT NULL PRIMARY KEY,
	year integer,
	month integer,
	day integer,
    dayofweek integer,
	hour integer, 
	minute integer, 
	date_time timestamp without time zone
);
ALTER TABLE dimDate OWNER TO postgres;




--
-- Name: factRental; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE factRental (
    payment_id integer NOT NULL PRIMARY KEY,
    customer_id integer NOT NULL,
    staff_id integer NOT NULL,
    film_id integer NOT NULL,
    date_id character varying(30) NOT NULL,
    amount numeric(5,2) NOT NULL,
    payment_date timestamp without time zone NOT NULL,
    rental_date timestamp without time zone NOT NULL,
    return_date timestamp without time zone
);
ALTER TABLE factRental OWNER TO postgres;



ALTER TABLE factRental
ADD CONSTRAINT fk_fact_dimFilm
FOREIGN KEY (film_id)
REFERENCES dimFilm(film_id);

ALTER TABLE factRental
ADD CONSTRAINT fk_fact_dimStore
FOREIGN KEY (staff_id)
REFERENCES dimStore(staff_id);

ALTER TABLE factRental
ADD CONSTRAINT fk_fact_dimCustomer
FOREIGN KEY (customer_id)
REFERENCES dimCustomer(customer_id);


ALTER TABLE factRental
ADD CONSTRAINT fk_fact_dimDate
FOREIGN KEY (date_id)
REFERENCES dimDate(date_id);



'''
