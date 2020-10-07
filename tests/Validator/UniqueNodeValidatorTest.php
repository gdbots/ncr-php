<?php
declare(strict_types=1);

namespace Gdbots\Tests\Ncr;

use Acme\Schemas\Forms\Node\FormV1;
use Gdbots\Ncr\Exception\NodeAlreadyExists;
use Gdbots\Ncr\GetNodeRequestHandler;
use Gdbots\Ncr\UniqueNodeValidator;
use Gdbots\Pbj\Exception\AssertionFailed;
use Gdbots\Pbj\WellKnown\NodeRef;
use Gdbots\Pbjx\Event\PbjxEvent;
use Gdbots\Schemas\Ncr\Command\CreateNodeV1;
use Gdbots\Schemas\Ncr\Command\RenameNodeV1;
use Gdbots\Schemas\Ncr\Command\UpdateNodeV1;
use Gdbots\Schemas\Ncr\Event\NodeCreatedV1;
use Gdbots\Schemas\Ncr\Request\GetNodeRequestV1;
use Gdbots\Schemas\Pbjx\StreamId;

final class UniqueNodeValidatorTest extends AbstractPbjxTest
{
    protected function setup(): void
    {
        parent::setup();
        $this->locator->registerRequestHandler(
            GetNodeRequestV1::schema()->getCurie(),
            new GetNodeRequestHandler($this->ncr)
        );
    }

    public function testValidateCreateNodeThatDoesNotExist(): void
    {
        $node = FormV1::create();
        $command = CreateNodeV1::create()->set('node', $node);
        $pbjxEvent = new PbjxEvent($command);
        (new UniqueNodeValidator())->validateCreateNode($pbjxEvent);

        // if it gets here then it's a pass
        $this->assertTrue(true);
    }

    public function testValidateCreateNodeThatDoesExistBySlug(): void
    {
        $existingNode = FormV1::create()->set('slug', 'thylacine-daydream');
        $newNode = FormV1::create()->set('slug', $existingNode->get('slug'));
        $this->ncr->putNode($existingNode);
        $command = CreateNodeV1::create()->set('node', $newNode);
        $this->expectException(NodeAlreadyExists::class);
        (new UniqueNodeValidator())->validateCreateNode(new PbjxEvent($command));
    }

    public function testValidateCreateNodeThatDoesExistById(): void
    {
        $node = FormV1::create();
        $this->eventStore->putEvents(
            StreamId::fromNodeRef(NodeRef::fromNode($node)),
            [NodeCreatedV1::create()->set('node', $node)]
        );
        $command = CreateNodeV1::create()->set('node', $node);
        $this->expectException(NodeAlreadyExists::class);
        (new UniqueNodeValidator())->validateCreateNode(new PbjxEvent($command));
    }

    public function testValidateUpdateNode(): void
    {
        $command = UpdateNodeV1::create()->set('new_node', FormV1::create());
        (new UniqueNodeValidator())->validateUpdateNode(new PbjxEvent($command));

        // if it gets here then it's a pass
        $this->assertTrue(true);
    }

    public function testValidateUpdateNodeSlugIsCopied(): void
    {
        $oldNode = FormV1::create()->set('slug', 'thylacine');
        $newNode = FormV1::create()->set('slug', 'daydream');
        $command = UpdateNodeV1::create()
            ->set('old_node', $oldNode)
            ->set('new_node', $newNode);
        (new UniqueNodeValidator())->validateUpdateNode(new PbjxEvent($command));
        $this->assertSame($oldNode->get('slug'), $command->get('new_node')->get('slug'));
    }

    public function testValidateUpdateNodeWithoutNewNode(): void
    {
        $command = UpdateNodeV1::create()->set('old_node', FormV1::create());
        $this->expectException(AssertionFailed::class);
        (new UniqueNodeValidator())->validateUpdateNode(new PbjxEvent($command));
    }

    public function testValidateRenameNode(): void
    {
        $node = FormV1::create();
        $command = RenameNodeV1::create()
            ->set('node_ref', NodeRef::fromNode($node))
            ->set('new_slug', 'thylacine-daydream');
        (new UniqueNodeValidator())->validateRenameNode(new PbjxEvent($command));

        // if it gets here then it's a pass
        $this->assertTrue(true);
    }

    public function testValidateRenameNodeWithoutNodeRef(): void
    {
        $command = RenameNodeV1::create();
        $this->expectException(AssertionFailed::class);
        (new UniqueNodeValidator())->validateRenameNode(new PbjxEvent($command));
    }

    public function testValidateRenameNodeWithoutNewSlug(): void
    {
        $node = FormV1::create();
        $command = RenameNodeV1::create()->set('node_ref', NodeRef::fromNode($node));
        $this->expectException(AssertionFailed::class);
        (new UniqueNodeValidator())->validateRenameNode(new PbjxEvent($command));
    }
}
