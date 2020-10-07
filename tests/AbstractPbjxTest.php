<?php
declare(strict_types=1);

namespace Gdbots\Tests\Ncr;

use Gdbots\Ncr\Repository\InMemoryNcr;
use Gdbots\Pbj\Message;
use Gdbots\Pbjx\EventStore\InMemoryEventStore;
use Gdbots\Pbjx\Pbjx;
use Gdbots\Pbjx\RegisteringServiceLocator;
use Gdbots\Pbjx\Scheduler\Scheduler;
use PHPUnit\Framework\TestCase;

abstract class AbstractPbjxTest extends TestCase
{
    protected RegisteringServiceLocator $locator;
    protected Pbjx $pbjx;
    protected InMemoryEventStore $eventStore;
    protected InMemoryNcr $ncr;
    protected Scheduler $scheduler;

    /**
     * Run the test
     */
    protected function setup(): void
    {
        $this->locator = new RegisteringServiceLocator();
        $this->pbjx = $this->locator->getPbjx();
        $this->eventStore = new InMemoryEventStore($this->pbjx);
        $this->locator->setEventStore($this->eventStore);
        $this->ncr = new InMemoryNcr();

        $this->scheduler = new class implements Scheduler
        {
            public $lastSendAt;
            public $lastCancelJobs;

            public function createStorage(array $context = []): void
            {
                //
            }

            public function describeStorage(array $context = []): string
            {
                return '';
            }

            public function sendAt(Message $command, int $timestamp, ?string $jobId = null, array $context = []): string
            {
                $this->lastSendAt = [
                    'command'   => $command,
                    'timestamp' => $timestamp,
                    'job_id'    => $jobId,
                ];
                return $jobId ?: 'jobid';
            }

            public function cancelJobs(array $jobIds, array $context = []): void
            {
                $this->lastCancelJobs = $jobIds;
            }
        };

        $this->locator->setScheduler($this->scheduler);
    }
}
