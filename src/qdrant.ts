import { QdrantClient } from '@qdrant/js-client-rest';
import { db } from './db';
import { embed } from './embed';

const qdrant = new QdrantClient({
  url: process.env.QDRANT_URL!,
  apiKey: process.env.QDRANT_API_KEY || undefined,
});

const COLLECTION = 'querynest_posts';

/**
 * Creates the Qdrant collection if it does not already exist and sets up
 * metadata indexes for efficient filtered search.
 */
export async function ensureCollection() {
  const exists = await qdrant.collectionExists(COLLECTION);

  if (!exists.exists) {
    await qdrant.createCollection(COLLECTION, {
      vectors: {
        size: 1536,
        distance: 'Cosine',
      },
      hnsw_config: { m: 16, ef_construct: 128 },
    });

    // Create payload indexes once, immediately after collection creation.
    await qdrant.createPayloadIndex(COLLECTION, {
      field_name: 'status',
      field_schema: 'keyword',
      wait: true,
    });

    await qdrant.createPayloadIndex(COLLECTION, {
      field_name: 'community_id',
      field_schema: 'integer',
      wait: true,
    });
  }
}

/**
 * Migrates posts from PostgreSQL into Qdrant in batches.
 */
export async function migratePosts() {
  await ensureCollection();

  const BATCH = 500;
  let offset = 0;

  while (true) {
    const { rows } = await db.query(
      `SELECT p.id AS post_id, pe.embedding, p.status, p.community_id, p.author_id, p.language
       FROM posts_embeddings pe
       JOIN posts p ON p.id = pe.post_id
       ORDER BY pe.id
       LIMIT $1 OFFSET $2`,
      [BATCH, offset]
    );

    if (rows.length === 0) break;

    const points = rows.map((row: {
      post_id: number | string;
      embedding: number[] | string;
      status: string;
      community_id: number;
      author_id: number;
      language: string;
    }) => {
      const vector = typeof row.embedding === 'string'
        ? JSON.parse(row.embedding)
        : row.embedding;

      return {
        id: Number(row.post_id),
        vector,
        payload: {
          post_id: Number(row.post_id),
          status: row.status,
          community_id: row.community_id,
          author_id: row.author_id,
          language: row.language,
        },
      };
    });

    await qdrant.upsert(COLLECTION, {
      wait: true,
      points,
    });

    offset += rows.length;
    console.log(`Migrated ${offset} points`);
  }
}

/**
 * Runs filtered semantic search. All metadata conditions must match.
 */
export async function filteredSearch(
  query: string,
  communityId: number,
  limit = 10
) {
  await ensureCollection();
  const qVec = await embed(query);

  const results = await qdrant.search(COLLECTION, {
    vector: qVec,
    limit,
    with_payload: true,
    filter: {
      must: [
        { key: 'status', match: { value: 'published' } },
        { key: 'community_id', match: { value: communityId } },
        { key: 'language', match: { value: 'en' } },
      ],
    },
  });

  return results.map(r => ({
    postId: (r.payload as any)?.post_id as number,
    score: Number(r.score.toFixed(4)),
    status: (r.payload as any)?.status,
    communityId: (r.payload as any)?.community_id,
    authorId: (r.payload as any)?.author_id,
  }));
}
