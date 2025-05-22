<?php

namespace Akeneo\Pim\Enrichment\Component\Product\Connector\Writer\File\Csv;

use Akeneo\Pim\Enrichment\Component\Product\Connector\FlatTranslator\FlatTranslatorInterface;
use Akeneo\Pim\Enrichment\Component\Product\Connector\Writer\File\GenerateFlatHeadersFromAttributeCodesInterface;
use Akeneo\Pim\Enrichment\Component\Product\Connector\Writer\File\GenerateFlatHeadersFromFamilyCodesInterface;
use Akeneo\Pim\Structure\Component\Repository\AttributeRepositoryInterface;
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
        $ignoredAttributes = $this->getIgnoredAttributes();
        $notPublic = $this->getNotPublicAttributeCodes();

        $this->hasItems = true;
        foreach ($items as &$item) {
            if ($this->entityManager && $notPublic) {
                foreach ($item['values'] as $valueKey => $value) {
                    if (\in_array($valueKey, $ignoredAttributes, false)) {
                        unset($item['values'][$valueKey]);
                        continue;
                    }

                    if (\in_array($valueKey, $notPublic, false)) {
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

        $ignoredAttributes = $this->getIgnoredAttributes();
        $notPublic = $this->getNotPublicAttributeCodes();
        $headerStrings = \array_filter($headerStrings, function ($headerString) use ($notPublic, $ignoredAttributes) {
            if (\in_array($headerString, $ignoredAttributes, false)) {
                return false;
            }

            return !\in_array($headerString, $notPublic, false);
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
    private function getIgnoredAttributes(): array
    {
        return [
            'base_image',
            'categories',
            'document',
            'ean',
            'enabled',
            'family',
            'groups',
            'image_extra_1',
            'image_extra_2',
            'image_extra_3',
            'image_extra_4',
            'image_extra_5',
            'image_extra_6',
            'image_extra_7',
            'image_extra_8',
            'image_extra_9',
            'image_extra_10',
            'image_extra_11',
            'image_extra_12',
            'image_extra_13',
            'image_extra_14',
            'image_extra_15',
            'image_extra_16',
            'image_extra_17',
            'image_extra_18',
            'image_extra_19',
            'image_extra_20',
            'image_extra_21',
            'image_extra_22',
            'image_extra_23',
            'image_extra_24',
            'image_extra_25',
            'image_extra_26',
            'image_extra_27',
            'image_extra_28',
            'image_extra_29',
            'image_extra_30',
            'toldkode',
        ];
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
