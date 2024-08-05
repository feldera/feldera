<script lang="ts">
  import { goto } from '$app/navigation'
  import { base } from '$app/paths'
  import { usePipelineList } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import { postPipeline } from '$lib/services/pipelineManager'

  type Example = { name: string; title: string; description: string; code: string }
  const examples: Example[] = [
    {
      name: 'product availability',
      title: 'Product Availability',
      description:
        'A demonstration of product availability across warehouses as it is constantly updated.',
      code: '-- Warehouse\nCREATE TABLE warehouse (\n    id INT PRIMARY KEY,\n    name VARCHAR NOT NULL,\n    address VARCHAR NOT NULL\n);\n\n-- Product\nCREATE TABLE product (\n    id INT PRIMARY KEY,\n    name VARCHAR NOT NULL,\n    mass DOUBLE NOT NULL,\n    volume DOUBLE NOT NULL\n);\n\n-- Each warehouse stores products\nCREATE TABLE storage (\n    warehouse_id INT FOREIGN KEY REFERENCES warehouse(id),\n    product_id INT FOREIGN KEY REFERENCES product(id),\n    num_available INT NOT NULL,\n    updated_at TIMESTAMP NOT NULL,\n    PRIMARY KEY (warehouse_id, product_id)\n);\n\n-- How much of each product is stored\nCREATE VIEW product_stored AS\n(\n    SELECT   product.id,\n             product.name,\n             SUM(storage.num_available) AS num_available,\n             SUM(storage.num_available * product.mass) AS total_mass,\n             SUM(storage.num_available * product.volume) AS total_volume\n    FROM     product\n             LEFT JOIN storage ON product.id = storage.product_id\n             LEFT JOIN warehouse ON storage.warehouse_id = warehouse.id\n    GROUP BY (product.id, product.name)\n);\n\n-- How much each warehouse has stored\nCREATE VIEW warehouse_stored AS\n(\n    SELECT   warehouse.id,\n             warehouse.name,\n             SUM(storage.num_available) AS num_available,\n             SUM(storage.num_available * product.mass) AS total_mass,\n             SUM(storage.num_available * product.volume) AS total_volume\n    FROM     warehouse\n             LEFT JOIN storage ON warehouse.id = storage.warehouse_id\n             LEFT JOIN product ON storage.product_id = product.id\n    GROUP BY (warehouse.id, warehouse.name)\n);\n\n-- Top 3 warehouse according to stored mass\nCREATE VIEW top_3_warehouse_mass AS\n(\n    SELECT   warehouse_stored.id,\n             warehouse_stored.name,\n             warehouse_stored.total_mass\n    FROM     warehouse_stored\n    ORDER BY warehouse_stored.total_mass DESC\n    LIMIT    3\n);\n\n-- Top 3 warehouse according to stored volume\nCREATE VIEW top_3_warehouse_volume AS\n(\n    SELECT   warehouse_stored.id,\n             warehouse_stored.name,\n             warehouse_stored.total_volume\n    FROM     warehouse_stored\n    ORDER BY warehouse_stored.total_volume DESC\n    LIMIT    3\n);\n\n-- Availability stats across all products\nCREATE VIEW product_availability AS\n(\n    SELECT COUNT(*) AS num_product,\n           MIN(product_stored.num_available) AS min_availability,\n           AVG(product_stored.num_available) AS avg_availability,\n           MAX(product_stored.num_available) AS max_availability,\n           SUM(product_stored.num_available) AS sum_availability\n    FROM   product_stored\n);\n\n-- Total number of warehouses\nCREATE VIEW num_warehouse AS\nSELECT COUNT(*) AS num_warehouse\nFROM   warehouse;\n\n-- Total number of products\nCREATE VIEW num_product AS\nSELECT COUNT(*) AS num_product\nFROM   product;\n\n-- Total number of storage entries\nCREATE VIEW num_storage AS\nSELECT COUNT(*) AS num_storage\nFROM   storage;\n'
    },
    undefined!,
    undefined!,
    undefined!
  ]
    .copyWithin(1, 0, 1)
    .copyWithin(2, 1, 2)
    .copyWithin(3, 2, 3)

  const pipelines = usePipelineList()
  const tryPipelineFromExample = async (example: Example) => {
    if (!pipelines.pipelines.some((p) => p.name === example.name)) {
      const newPipeline = await postPipeline({
        name: example.name,
        runtime_config: {},
        program_config: {},
        description: example.description,
        program_code: example.code
      })
      pipelines.pipelines.push(newPipeline)
    }
    goto(`${base}/pipelines/${encodeURIComponent(example.name)}/`)
  }
</script>

<div class="h5 px-8 py-8 font-normal md:px-16">
  Try running one of our examples below, or writing a new pipeline from scratch:
  <button
    class="btn mt-auto self-end text-sm preset-filled-primary-500"
    onclick={() => goto('#new')}
  >
    CREATE NEW PIPELINE
  </button>
</div>
<div class="grid grid-cols-1 gap-8 px-8 sm:grid-cols-2 md:gap-16 md:px-16 lg:grid-cols-3">
  <!-- <div class="card flex h-48 flex-col bg-white p-4 dark:bg-black">
    <button
      class="btn mt-auto self-end text-sm preset-filled-primary-500"
      onclick={() => goto('#new')}
    >
      CREATE PIPELINE
      <div class="bx bx-right-arrow-alt text-[24px]"></div>
    </button>
  </div> -->
  {#each examples as example}
    <div class="card flex flex-col gap-2 bg-white p-4 dark:bg-black">
      <span class="h5 font-normal">{example.title}</span>
      <span class="text-left">{example.description}</span>
      <button
        onclick={() => tryPipelineFromExample(example)}
        class="btn mt-auto self-end text-sm preset-filled-primary-500"
      >
        TRY
        <div class="bx bx-right-arrow-alt text-[24px]"></div>
      </button>
    </div>
  {/each}
</div>
