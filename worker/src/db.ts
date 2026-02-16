import pg from 'pg';
import 'dotenv/config';

const { Pool } = pg;

let pool: pg.Pool | null = null;

export const getPool = () => {
  if (!pool) {
    const connectionString = process.env.DATABASE_URL;
    if (!connectionString) {
      throw new Error('DATABASE_URL is not set');
    }
    pool = new Pool({ connectionString });
  }
  return pool;
};

export const query = async <T = unknown>(text: string, params: unknown[] = []) => {
  const client = await getPool().connect();
  try {
    const result = await client.query<T>(text, params);
    return result;
  } finally {
    client.release();
  }
};
