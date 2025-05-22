<?php

namespace Akeneo\Pim\Enrichment\Component\Product\Connector\Writer\File\Csv;

use Akeneo\Pim\Enrichment\Component\Product\Connector\FlatTranslator\FlatTranslatorInterface;
use Akeneo\Pim\Enrichment\Component\Product\Connector\Writer\File\GenerateFlatHeadersFromAttributeCodesInterface;
use Akeneo\Pim\Enrichment\Component\Product\Connector\Writer\File\GenerateFlatHeadersFromFamilyCodesInterface;
use Akeneo\Pim\Structure\Component\Repository\AttributeRepositoryInterface;
use Akeneo\Tool\Component\Batch\Item\InitializableInterface;
use Akeneo\Tool\Component\Batch\Item\ItemWriterInterface;
use Akeneo\Tool\Component\Batch\Job\JobParameters;
use Akeneo\Tool\Component\Buffer\BufferFactory;
use Akeneo\Tool\Component\Connector\ArrayConverter\ArrayConverterInterface;
use Akeneo\Tool\Component\Connector\Writer\File\AbstractItemMediaWriter;
use Akeneo\Tool\Component\Connector\Writer\File\FileExporterPathGeneratorInterface;
use Akeneo\Tool\Component\Connector\Writer\File\FlatItemBufferFlusher;
use Akeneo\Tool\Component\FileStorage\FilesystemProvider;
use Akeneo\Tool\Component\FileStorage\Repository\FileInfoRepositoryInterface;
use Doctrine\ORM\EntityManagerInterface;

/**
 * Write product data into a csv file on the local filesystem
 *
 * @author    Yohan Blain <yohan.blain@akeneo.com>
 * @copyright 2015 Akeneo SAS (http://www.akeneo.com)
 * @license   http://opensource.org/licenses/osl-3.0.php  Open Software License (OSL 3.0)
 */
class ProductWriter extends AbstractItemMediaWriter
{
    protected GenerateFlatHeadersFromFamilyCodesInterface $generateHeadersFromFamilyCodes;
    protected GenerateFlatHeadersFromAttributeCodesInterface $generateHeadersFromAttributeCodes;
    protected ?EntityManagerInterface $entityManager;

    protected array $familyCodes = [];
    private bool $hasItems = false;

    public function __construct(
        ArrayConverterInterface $arrayConverter,
        BufferFactory $bufferFactory,
        FlatItemBufferFlusher $flusher,
        AttributeRepositoryInterface $attributeRepository,
        FileExporterPathGeneratorInterface $fileExporterPath,
        GenerateFlatHeadersFromFamilyCodesInterface $generateHeadersFromFamilyCodes,
        GenerateFlatHeadersFromAttributeCodesInterface $generateHeadersFromAttributeCodes,
        FlatTranslatorInterface $flatTranslator,
        FileInfoRepositoryInterface $fileInfoRepository,
        FilesystemProvider $filesystemProvider,
        array $mediaAttributeTypes,
        string $jobParamFilePath = self::DEFAULT_FILE_PATH,
        ?EntityManagerInterface $manager = null,
    ) {
        parent::__construct(
            $arrayConverter,
            $bufferFactory,
            $flusher,
            $attributeRepository,
            $fileExporterPath,
            $flatTranslator,
            $fileInfoRepository,
            $filesystemProvider,
            $mediaAttributeTypes,
            $jobParamFilePath
        );

        $this->generateHeadersFromFamilyCodes = $generateHeadersFromFamilyCodes;
        $this->generateHeadersFromAttributeCodes = $generateHeadersFromAttributeCodes;
        $this->entityManager = $manager;
    }

    /**
     * {@inheritdoc}
     */
    public function initialize(): void
    {
        $this->familyCodes = [];
        $this->hasItems = false;

        parent::initialize();
    }

    /**
     * {@inheritdoc}
     */
    public function write(array $items): void
    {
        $notPublic = $this->getNotPublicAttributeCodes();

        $this->hasItems = true;
        foreach ($items as &$item) {
            if ($this->entityManager && $notPublic) {
                foreach ($item['values'] as $valueKey => $value) {
                    if (\in_array($valueKey, $notPublic)) {
                        unset($item['values'][$valueKey]);
                    }
                }
            }

            if (isset($item['family']) && !in_array($item['family'], $this->familyCodes)) {
                $this->familyCodes[] = $item['family'];
            }
        }
        unset($item);

        parent::write($items);
    }

    /**
     * Return additional headers, based on the requested attributes if any,
     * and from the families definition
     */
    protected function getAdditionalHeaders(): array
    {
        $parameters = $this->stepExecution->getJobParameters();

        $filters = $parameters->get('filters');
        $localeCodes = isset($filters['structure']['locales']) ? $filters['structure']['locales'] : [$parameters->get('locale')];
        $channelCode = isset($filters['structure']['scope']) ? $filters['structure']['scope'] : $parameters->get('scope');

        $attributeCodes = [];

        if (isset($filters['structure']['attributes'])
            && !empty($filters['structure']['attributes'])
            && $this->hasItems === true) {
            $attributeCodes = $filters['structure']['attributes'];
        } elseif ($parameters->has('selected_properties')) {
            $attributeCodes = $parameters->get('selected_properties');
        }

        $headers = [];
        if (!empty($attributeCodes)) {
            $headers = ($this->generateHeadersFromAttributeCodes)($attributeCodes, $channelCode, $localeCodes);
        } elseif (!empty($this->familyCodes)) {
            $headers = ($this->generateHeadersFromFamilyCodes)($this->familyCodes, $channelCode, $localeCodes);
        }

        $withMedia = (!$parameters->has('with_media') || ($parameters->has('with_media') && $parameters->get('with_media')));

        $headerStrings = [];
        foreach ($headers as $header) {
            if ($withMedia || !$header->isMedia()) {
                $headerStrings = array_merge(
                    $headerStrings,
                    $header->generateHeaderStrings()
                );
            }
        }

        $notPublic = $this->getNotPublicAttributeCodes();
        $headerStrings = \array_filter($headerStrings, function ($headerString) use ($notPublic) {
            return !\in_array($headerString, $notPublic);
        });

        return $headerStrings;
    }

    /**
     * {@inheritdoc}
     */
    protected function getWriterConfiguration(): array
    {
        $parameters = $this->stepExecution->getJobParameters();

        return [
            'type'           => 'csv',
            'fieldDelimiter' => $parameters->get('delimiter'),
            'fieldEnclosure' => $parameters->get('enclosure'),
            'shouldAddBOM'   => false,
        ];
    }

    /**
     * {@inheritdoc}
     */
    protected function getItemIdentifier(array $item): string
    {
        return $item['identifier'] ?? $item['uuid'];
    }

    /**
     * {@inheritdoc}
     */
    protected function getConverterOptions(JobParameters $parameters): array
    {
        $converterOptions =  parent::getConverterOptions($parameters);
        if ($parameters->has('with_uuid')) {
            $converterOptions += ['with_uuid' => $parameters->get('with_uuid')];
        }

        return $converterOptions;
    }

    /**
     * @return string[]
     */
    private function getNotPublicAttributeCodes(): array
    {
        if (!$this->entityManager) {
            return [];
        }

        $sql = <<<SQL
SELECT a.code FROM pim_catalog_attribute a
INNER JOIN pim_catalog_attribute_group ag ON ag.id = a.group_id
WHERE ag.code = 'notpublic'
SQL;
        $connection = $this->entityManager->getConnection();
        $stmt = $connection->prepare($sql);
        return $stmt->executeQuery()->fetchFirstColumn();
    }
}
