SET client_encoding = 'UTF8';

CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS provsql WITH SCHEMA public;


-- implementation for Formula semiring

DROP TYPE IF EXISTS public.formula_state CASCADE;
CREATE TYPE public.formula_state AS (
	formula text,
	nbargs integer
);

DROP FUNCTION IF EXISTS public.formula_monus(text, text) CASCADE;
CREATE FUNCTION public.formula_monus(formula1 text, formula2 text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
  SELECT concat('(',formula1,' ⊖ ',formula2,')')
$$;

DROP FUNCTION IF EXISTS public.formula_plus_state(public.formula_state, text) CASCADE;
CREATE FUNCTION public.formula_plus_state(state public.formula_state, value text) RETURNS public.formula_state
    LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
     AS $$
BEGIN
  IF state IS NULL OR state.nbargs=0 THEN
    RETURN (value,1);
  ELSE
    RETURN (concat(state.formula,' ⊕ ',value),state.nbargs+1);
  END IF;
END
$$;

DROP FUNCTION IF EXISTS public.formula_state2formula(public.formula_state) CASCADE;
CREATE FUNCTION public.formula_state2formula(state public.formula_state) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
  SELECT
    CASE
      WHEN state.nbargs<2 THEN state.formula
      ELSE concat('(',state.formula,')')
    END;
$$;

DROP FUNCTION IF EXISTS public.formula_times_state(public.formula_state, text) CASCADE;
CREATE FUNCTION public.formula_times_state(state public.formula_state, value text) RETURNS public.formula_state
    LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
    AS $$
BEGIN    
  IF state IS NULL OR state.nbargs=0 THEN
    RETURN (value,1);
  ELSE
    RETURN (concat(state.formula,' ⊗ ',value),state.nbargs+1);
  END IF;
END
$$;

DROP FUNCTION IF EXISTS public.formula_delta(text) CASCADE;
CREATE FUNCTION public.formula_delta(formula text) RETURNS text
    LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
    AS $$
BEGIN    
  RETURN (SELECT concat('δ(',formula,')'));
END
$$;

DROP AGGREGATE IF EXISTS public.formula_plus(text) CASCADE;
CREATE AGGREGATE public.formula_plus(text) (
    SFUNC = public.formula_plus_state,
    STYPE = public.formula_state,
    INITCOND = '(𝟘,0)',
    FINALFUNC = public.formula_state2formula
);

DROP AGGREGATE IF EXISTS public.formula_times(text) CASCADE;
CREATE AGGREGATE public.formula_times(text) (
    SFUNC = public.formula_times_state,
    STYPE = public.formula_state,
    INITCOND = '(𝟙,0)',
    FINALFUNC = public.formula_state2formula
);

DROP FUNCTION IF EXISTS public.formula(UUID, regclass) CASCADE;
CREATE FUNCTION public.formula(token UUID, token2value regclass) RETURNS text
    LANGUAGE plpgsql PARALLEL SAFE
    AS $$
BEGIN
  RETURN provenance_evaluate(
    token,
    token2value,
    '𝟙'::text,
    'formula_plus',
    'formula_times',
    'formula_monus',
    'formula_delta');
END
$$;



-- implementation for Counting semiring

DROP FUNCTION IF EXISTS public.counting(UUID, regclass) CASCADE;
CREATE FUNCTION public.counting(token UUID, token2value regclass) RETURNS integer
    LANGUAGE plpgsql PARALLEL SAFE
    AS $$
BEGIN
  RETURN provenance_evaluate(
    token,
    token2value,
    1,
    'counting_plus',
    'counting_times',
    'counting_monus',
    'counting_delta');
END
$$;

DROP FUNCTION IF EXISTS public.counting_monus(integer, integer) CASCADE;
CREATE FUNCTION public.counting_monus(counting1 integer, counting2 integer) RETURNS integer
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
  SELECT CASE WHEN counting1 < counting2 THEN 0 ELSE counting1 - counting2 END
$$;

DROP FUNCTION IF EXISTS public.counting_plus_state(integer, integer) CASCADE;
CREATE FUNCTION public.counting_plus_state(state integer, value integer) RETURNS integer
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
  SELECT CASE WHEN state IS NULL THEN value ELSE state + value END
$$;

DROP FUNCTION IF EXISTS public.counting_times_state(integer, integer) CASCADE;
CREATE FUNCTION public.counting_times_state(state integer, value integer) RETURNS integer
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
SELECT CASE WHEN state IS NULL THEN value ELSE state * value END
$$;

DROP AGGREGATE IF EXISTS public.counting_plus(integer) CASCADE;
CREATE AGGREGATE public.counting_plus(integer) (
    SFUNC = public.counting_plus_state,
    STYPE = integer,
    INITCOND = '0'
);

DROP AGGREGATE IF EXISTS public.counting_times(integer) CASCADE;
CREATE AGGREGATE public.counting_times(integer) (
    SFUNC = public.counting_times_state,
    STYPE = integer,
    INITCOND = '1'
);

DROP FUNCTION IF EXISTS public.counting_delta(integer) CASCADE;
CREATE FUNCTION public.counting_delta(counting integer) RETURNS integer
    LANGUAGE sql IMMUTABLE STRICT
    AS $$
  SELECT CASE WHEN counting > 0 THEN 1 ELSE 1 END
$$;

-- semiring implementation for why provenance 



CREATE OR REPLACE FUNCTION public.fmonus(state1 text[], state2 text[]) RETURNS text[]
    LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
BEGIN
    IF state1 IS NULL THEN
        RETURN ARRAY[]::text[];
    ELSIF state2 IS NULL THEN
        RETURN state1
        ;
    ELSE
        RETURN ARRAY(
            SELECT unnest(state1)
            EXCEPT
            SELECT unnest(state2)
        );

        
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION public.whyPROV_now_plus_state(state text[], value text[]) RETURNS text[]
    LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
    AS $$
BEGIN
    IF state IS NULL THEN
        RETURN value;
    ELSE
        RETURN array(SELECT unnest(state) UNION SELECT unnest(value));
    END IF;
END
$$;



CREATE OR REPLACE FUNCTION public.whyPROV_now_times_state(state text[], value text[]) RETURNS text[]
    LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
    AS $$
BEGIN
    IF state IS NULL THEN
        RETURN value;
    ELSE
        RETURN array(SELECT '{' || array_to_string(ARRAY( SELECT DISTINCT UNNEST(s::text[] || v::text[]) x ORDER BY x), ',') || '}' FROM unnest(state) s,unnest(value) v );
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION public.whyPROV_now_delta(state text[]) RETURNS text[]
 LANGUAGE plpgsql IMMUTABLE
    AS $$
BEGIN
  IF state IS NULL OR array_length(state,1)=0 THEN RETURN '{}'::text[]; --el 0 of semiring
  ELSE RETURN '{"{}"}'::text[]; --el 1 of semiring
  END IF;
END

$$;


CREATE OR REPLACE AGGREGATE public.whyPROV_now_plus(text[]) (
    SFUNC = public.whyPROV_now_plus_state,
    STYPE = text[],
    INITCOND = '{}'
    
);


CREATE OR REPLACE AGGREGATE public.whyPROV_now_times(text[]) (
    SFUNC = public.whyPROV_now_times_state,
    STYPE = text[],
    INITCOND = '{"{}"}'
   
);


CREATE OR REPLACE FUNCTION public.whyPROV_now(token UUID, token2value regclass) RETURNS text
    LANGUAGE plpgsql PARALLEL SAFE
    AS $$
BEGIN
  RETURN provenance_evaluate(
    token,
    token2value,
    '{"{}"}'::text[],
    'whyPROV_now_plus',
    'whyPROV_now_times',
    'fmonus',
    'whyPROV_now_delta');
END
$$;

DROP FUNCTION IF EXISTS formula_semimod(text, text) CASCADE;
CREATE FUNCTION formula_semimod(formula1 text, formula2 text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    AS $$
  SELECT concat('(',formula1,' * ',formula2,')')
$$;

DROP FUNCTION IF EXISTS formula_agg_state(formula_state, text) CASCADE;
CREATE FUNCTION formula_agg_state(state formula_state, value text) RETURNS formula_state
    LANGUAGE plpgsql IMMUTABLE
    AS $$
BEGIN
  IF state IS NULL OR state.nbargs=0 THEN
    RETURN (value,1);
  ELSE
    RETURN (concat(state.formula,' , ',value),state.nbargs+1);
  END IF;
END
$$;

DROP AGGREGATE IF EXISTS formula_agg(text) CASCADE;
CREATE AGGREGATE formula_agg(text) (
    SFUNC = formula_agg_state,
    STYPE = formula_state,
    INITCOND = '(1,0)'
);

DROP FUNCTION IF EXISTS formula_agg_final(formula_state, varchar) CASCADE;
CREATE FUNCTION formula_agg_final(state formula_state, fname varchar) RETURNS text
  LANGUAGE sql IMMUTABLE STRICT
  AS
  $$
    SELECT concat(fname,'{ ',state.formula,' }');
  $$;

DROP FUNCTION IF EXISTS aggregation_formula(anyelement, regclass) CASCADE;
CREATE FUNCTION aggregation_formula(token anyelement, token2value regclass) RETURNS text
    LANGUAGE plpgsql
    AS $$
BEGIN
  RETURN provsql.aggregation_evaluate(
    token,
    token2value,
    'formula_agg_final',
    'formula_agg',
    'formula_semimod',
    '𝟙'::text,
    'formula_plus',
    'formula_times',
    'formula_monus',
    'formula_delta');
END
$$;

