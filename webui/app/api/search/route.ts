import { NextRequest, NextResponse } from 'next/server'
import API from '@searchkit/api'

const apiClient = API(
  {
    connection: {
      host: `${process.env.ELASTIC_SEARCH_SCHEME}://${process.env.ELASTIC_HOST}:${process.env.ELASTIC_PORT}`,
    },
    search_settings: {
      search_attributes: [
        { field: 'content', weight: 10 },
        { field: 'hash', weight: 10 },
      ],
      result_attributes: ['content', 'title', 'chunk_number', 'hash'],
    }
  },
  { debug: false }
)

export async function POST(req: NextRequest, res: NextResponse) {
  const data = await req.json()

  const results = await apiClient.handleRequest(data)
  return NextResponse.json(results)
}
