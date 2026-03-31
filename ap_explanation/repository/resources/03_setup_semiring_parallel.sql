SET client_encoding = 'UTF8';

-- implementation for Formula semiring
DROP TYPE IF EXISTS public.formula_state;
CREATE TYPE public.formula_state AS (
	formula text,
	nbargs integer
);

CREATE OR REPLACE FUNCTION public.formula_monus(formula1 text, formula2 text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
  SELECT concat('(',formula1,' ⊖ ',formula2,')')
$$;

CREATE OR REPLACE FUNCTION public.formula_plus_state(state public.formula_state, value text) RETURNS public.formula_state
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

CREATE OR REPLACE FUNCTION public.formula_state2formula(state public.formula_state) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
  SELECT
    CASE
      WHEN state.nbargs<2 THEN state.formula
      ELSE concat('(',state.formula,')')
    END;
$$;

CREATE OR REPLACE FUNCTION public.formula_times_state(state public.formula_state, value text) RETURNS public.formula_state
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

CREATE OR REPLACE FUNCTION public.formula_delta(formula text) RETURNS text
    LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
    AS $$
BEGIN    
  RETURN (SELECT concat('δ(',formula,')'));
END
$$;

CREATE OR REPLACE AGGREGATE public.formula_plus(text) (
    SFUNC = public.formula_plus_state,
    STYPE = public.formula_state,
    INITCOND = '(𝟘,0)',
    FINALFUNC = public.formula_state2formula
);

CREATE OR REPLACE AGGREGATE public.formula_times(text) (
    SFUNC = public.formula_times_state,
    STYPE = public.formula_state,
    INITCOND = '(𝟙,0)',
    FINALFUNC = public.formula_state2formula
);

CREATE OR REPLACE FUNCTION public.formula(token UUID, token2value regclass) RETURNS text
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

-- implementation for Boolean formula semiring
DROP TYPE IF EXISTS public.bool_formula_state;
CREATE TYPE public.bool_formula_state AS (
	formula text,
	nbargs integer
);

CREATE OR REPLACE FUNCTION public.bool_formula_monus(formula1 text, formula2 text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
  SELECT concat('(',formula1,' ∧ ¬',formula2,')');
$$;

CREATE OR REPLACE FUNCTION public.bool_formula_plus_state(state public.bool_formula_state, value text) RETURNS public.bool_formula_state
    LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
     AS $$
BEGIN
  IF state IS NULL OR state.nbargs=0 THEN
    RETURN (value,1);
  ELSE
    RETURN (concat(state.formula,' ∨ ',value),state.nbargs+1);
  END IF;
END
$$;

CREATE OR REPLACE FUNCTION public.bool_formula_state2formula(state public.bool_formula_state) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
  SELECT
    CASE
      WHEN state.nbargs<2 THEN state.formula
      ELSE concat('(',state.formula,')')
    END;
$$;

CREATE OR REPLACE FUNCTION public.bool_formula_times_state(state public.bool_formula_state, value text) RETURNS public.bool_formula_state
    LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
    AS $$
BEGIN    
  IF state IS NULL OR state.nbargs=0 THEN
    RETURN (value,1);
  ELSE
    RETURN (concat(state.formula,' ∧ ',value),state.nbargs+1);
  END IF;
END
$$;

CREATE OR REPLACE FUNCTION public.bool_formula_delta(formula text) RETURNS text
    LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
    AS $$
BEGIN    
  RETURN formula;
END
$$;

CREATE OR REPLACE AGGREGATE public.bool_formula_plus(text) (
    SFUNC = public.bool_formula_plus_state,
    STYPE = public.bool_formula_state,
    INITCOND = '(⊥,0)',
    FINALFUNC = public.bool_formula_state2formula
);

CREATE OR REPLACE AGGREGATE public.bool_formula_times(text) (
    SFUNC = public.bool_formula_times_state,
    STYPE = public.bool_formula_state,
    INITCOND = '(⊤,0)',
    FINALFUNC = public.bool_formula_state2formula
);

CREATE OR REPLACE FUNCTION public.bool_formula(token UUID, token2value regclass) RETURNS text
    LANGUAGE plpgsql PARALLEL SAFE
    AS $$
BEGIN
  RETURN provenance_evaluate(
    token,
    token2value,
    '⊤'::text,
    'bool_formula_plus',
    'bool_formula_times',
    'bool_formula_monus',
    'bool_formula_delta');
END
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

CREATE OR REPLACE FUNCTION formula_semimod(formula1 text, formula2 text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    AS $$
  SELECT concat('(',formula1,' * ',formula2,')')
$$;

CREATE OR REPLACE FUNCTION formula_agg_state(state formula_state, value text) RETURNS formula_state
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

CREATE OR REPLACE AGGREGATE formula_agg(text) (
    SFUNC = formula_agg_state,
    STYPE = formula_state,
    INITCOND = '(1,0)'
);

CREATE OR REPLACE FUNCTION formula_agg_final(state formula_state, fname varchar) RETURNS text
  LANGUAGE sql IMMUTABLE STRICT
  AS
  $$
    SELECT concat(fname,'{ ',state.formula,' }');
  $$;

CREATE OR REPLACE FUNCTION aggregation_formula(token anyelement, token2value regclass) RETURNS text
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

-- Canary table to track script execution
-- This table is used to verify that the semiring setup has been executed
CREATE TABLE IF NOT EXISTS public.provsql_canary (
    script_name VARCHAR(255) PRIMARY KEY,
    version VARCHAR(50) NOT NULL,
    executed_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Insert or update the canary record for this script
INSERT INTO public.provsql_canary (script_name, version, executed_at)
VALUES ('03_setup_semiring_parallel.sql', '1.0.0', NOW())
ON CONFLICT (script_name) 
DO UPDATE SET 
    version = EXCLUDED.version,
    executed_at = EXCLUDED.executed_at;

