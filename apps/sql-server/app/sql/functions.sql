-- These functions mostly pack up the parameters into a JSON object
-- This JSON object is intended to be directly compatible with the ApertureDB query language

-- Operations

-- Resize
CREATE OR REPLACE FUNCTION RESIZE(
    width integer DEFAULT NULL,
    height integer DEFAULT NULL,
    scale float DEFAULT NULL
)
RETURNS jsonb AS $$
  SELECT jsonb_strip_nulls(jsonb_build_object(
    'type', 'resize',
    'width', $1,
    'height', $2,
    'scale', $3
  ))
$$ LANGUAGE SQL IMMUTABLE;

-- Rotate
CREATE OR REPLACE FUNCTION ROTATE(
    angle float,
    resize bool DEFAULT false
)
RETURNS jsonb AS $$
  SELECT jsonb_build_object(
    'type', 'rotate',
    'angle', $1,
    'resize', $2
  )
$$ LANGUAGE SQL IMMUTABLE;

-- Crop
CREATE OR REPLACE FUNCTION CROP(x int, y int, width int, height int)
RETURNS jsonb AS $$
  SELECT jsonb_build_object(
    'type', 'crop',
    'x', $1,
    'y', $2,
    'width', $3,
    'height', $4
  )
$$ LANGUAGE SQL IMMUTABLE;

-- Threshold
CREATE OR REPLACE FUNCTION THRESHOLD(
    value integer
)
RETURNS jsonb AS $$
  SELECT jsonb_build_object(
    'type', 'threshold',
    'value', $1
  )
$$ LANGUAGE SQL IMMUTABLE;

-- Flip
CREATE OR REPLACE FUNCTION FLIP(
    code int
)
RETURNS jsonb AS $$
  SELECT jsonb_build_object(
    'type', 'flip',
    'code', $1
  )
$$ LANGUAGE SQL IMMUTABLE;

-- Interval
-- Note that INTERVAL is a reserved keyword in SQL, so we use OP_INTERVAL instead.
CREATE OR REPLACE FUNCTION OP_INTERVAL(
    start integer,
    stop integer,
    step integer
)
RETURNS jsonb AS $$
  SELECT jsonb_build_object(
    'type', 'interval',
    'start', $1,
    'stop', $2,
    'step', $3
  )
$$ LANGUAGE SQL IMMUTABLE;

-- Preview
CREATE OR REPLACE FUNCTION PREVIEW(
    max_frame_count integer DEFAULT NULL,
    max_time_fraction float DEFAULT NULL,
    max_time_offset float DEFAULT NULL,
    max_size_mb float DEFAULT NULL
)
RETURNS jsonb AS $$
  SELECT jsonb_strip_nulls(jsonb_build_object(
    'type', 'preview',
    'max_frame_count', $1,
    'max_time_fraction', $2,
    'max_time_offset', $3,
    'max_size_mb', $4
  ))
$$ LANGUAGE SQL IMMUTABLE;

-- Operations wrapper
CREATE OR REPLACE FUNCTION OPERATIONS(VARIADIC ops jsonb[])
RETURNS jsonb AS $$
  SELECT jsonb_agg(op) FROM unnest($1) AS op
$$ LANGUAGE SQL IMMUTABLE;