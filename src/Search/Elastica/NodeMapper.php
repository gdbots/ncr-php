<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Search\Elastica;

use Elastica\Document;
use Elastica\Type\Mapping;
use Gdbots\Pbj\Marshaler\Elastica\MappingFactory;
use Gdbots\Pbj\Message;
use Gdbots\Pbj\MessageResolver;
use Gdbots\Pbj\Schema;
use Gdbots\Pbj\SchemaQName;
use Gdbots\Schemas\Ncr\Mixin\Indexed\Indexed;
use Gdbots\Schemas\Ncr\Mixin\Node\NodeV1Mixin;

class NodeMapper
{
    /** @var MappingFactory */
    protected $mappingFactory;

    /**
     * The mappers are constructed with "new $class" in the
     * IndexManager so the constructor must be consistent.
     */
    final public function __construct()
    {
    }

    /**
     * @param SchemaQName $qname
     *
     * @return Mapping
     *
     * @throws \InvalidArgumentException
     */
    final public function getMapping(SchemaQName $qname): Mapping
    {
        /** @var Message $class */
        $class = MessageResolver::resolveCurie(MessageResolver::resolveQName($qname));
        $schema = $class::schema();
        $curie = NodeV1Mixin::create()->getId()->getCurieMajor();

        if (!$schema->hasMixin($curie)) {
            throw new \InvalidArgumentException(
                sprintf('The SchemaQName [%s] does not have mixin [%s].', $qname, $curie)
            );
        }

        $mapping = $this->getMappingFactory()->create($schema, $this->getDefaultAnalyzer());
        // elastica or elasticsearch throws exception when _id is in the properties
        // likely due to it being a builtin field.
        $properties = $mapping->getProperties();

        // we use multi-field indexing on title to create sortable field
        // on nodes for general search and listing (title.raw)
        if ('text' === $properties['title']['type']) {
            // elastica >=5 uses "text" for string type
            $properties['title']['fields'] = ['raw' => ['type' => 'keyword', 'normalizer' => 'pbj_keyword']];
        } else {
            // elastica <5 uses "string" for string type
            $properties['title']['fields'] = ['raw' => ['type' => 'string', 'analyzer' => 'pbj_keyword_analyzer']];
        }

        unset($properties['_id']);
        $mapping
            ->setAllField(['enabled' => true, 'analyzer' => $this->getDefaultAnalyzer()])
            ->setProperties($properties);

        $dynamicTemplates = $mapping->getParam('dynamic_templates');
        if (!empty($dynamicTemplates)) {
            $mapping->setParam('dynamic_templates', $dynamicTemplates);
        }

        $this->filterMapping($mapping, $schema);
        return $mapping;
    }

    /**
     * @return string
     */
    public function getDefaultAnalyzer(): ?string
    {
        return 'english';
    }

    /**
     * @return array
     */
    public function getCustomAnalyzers(): array
    {
        return MappingFactory::getCustomAnalyzers();
    }

    /**
     * @param Document $document
     * @param Indexed  $node
     */
    public function beforeIndex(Document $document, Indexed $node)
    {
        // Override to customize the document before it is indexed.
    }

    /**
     * @param Mapping $mapping
     * @param Schema  $schema
     */
    protected function filterMapping(Mapping $mapping, Schema $schema): void
    {
        // Override to customize the mapping
    }

    /**
     * @return MappingFactory
     */
    final protected function getMappingFactory(): MappingFactory
    {
        if (null === $this->mappingFactory) {
            $this->mappingFactory = $this->doGetMappingFactory();
        }

        return $this->mappingFactory;
    }

    /**
     * @return MappingFactory
     */
    protected function doGetMappingFactory(): MappingFactory
    {
        return new MappingFactory();
    }
}
