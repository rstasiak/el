<?php

namespace RStasiak\EL\Extract\Typesense;

use RStasiak\EL\Contract\ExtractorInterface;
use Typesense\Client;

class TypesenseExtractor implements ExtractorInterface
{
    private Client $client;

    public function __construct(Client $client)
    {
        $this->client = $client;
    }

    /**
     * Extract data from Typesense collection
     *
     * @param array $command [
     *   'collection' => 'collection_name',
     *   'query' => 'search query' (optional),
     *   'query_by' => 'field1,field2' (optional, required if query provided),
     *   'filter_by' => 'field:value' (optional),
     *   'sort_by' => 'field:asc' (optional),
     *   'per_page' => 250 (optional, max 250),
     *   'page' => 1 (optional),
     *   'limit' => null (optional, max results to return)
     * ]
     * @return array Extracted documents
     */
    public function extract(array $command): array
    {
        $collection = $command['collection'] ?? throw new \InvalidArgumentException('Collection name required');
        $query = $command['query'] ?? '*';
        $queryBy = $command['query_by'] ?? '';
        $filterBy = $command['filter_by'] ?? null;
        $sortBy = $command['sort_by'] ?? null;
        $perPage = min($command['per_page'] ?? 250, 250);
        $page = $command['page'] ?? 1;
        $limit = $command['limit'] ?? null;

        $results = [];
        $currentPage = $page;
        $totalFetched = 0;

        do {
            $searchParams = [
                'q' => $query,
                'per_page' => $perPage,
                'page' => $currentPage,
            ];

            if ($queryBy) {
                $searchParams['query_by'] = $queryBy;
            }

            if ($filterBy) {
                $searchParams['filter_by'] = $filterBy;
            }

            if ($sortBy) {
                $searchParams['sort_by'] = $sortBy;
            }

            $response = $this->client->collections[$collection]->documents->search($searchParams);

            foreach ($response['hits'] as $hit) {
                if ($limit !== null && $totalFetched >= $limit) {
                    break 2;
                }

                $results[] = $hit['document'];
                $totalFetched++;
            }

            $currentPage++;

            // Continue if there are more pages and we haven't hit the limit
        } while (
            $response['found'] > count($results) &&
            ($limit === null || $totalFetched < $limit)
        );

        return $results;
    }
}
