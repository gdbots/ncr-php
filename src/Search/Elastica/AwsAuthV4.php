<?php
declare(strict_types=1);

namespace Gdbots\Ncr\Search\Elastica;

use Aws\Credentials\CredentialProvider;
use Aws\Credentials\Credentials;
use Aws\Signature\SignatureV4;
use Elastica\Connection;
use Elastica\Transport\Guzzle;
use GuzzleHttp\Client;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\RequestOptions;
use GuzzleHttp\Utils;
use Psr\Http\Message\RequestInterface;

/**
 * This AwsAuthV4 is copied from Elastica's AwsAuthV4 and modified
 * since the original class doesn't allow us to override the methods needed.
 *
 * It's a temp hack until elastica lib provides ability to configure guzzle options
 * or at a minimum, the connect_timeout option.
 *
 * The getConnectTimeoutMiddleware is the "unique" feature here and
 * ensures we fail fast on connection attempts and along with ClientManager
 * re-enable the connections when they fail. We do this because AWS provides
 * a single master endpoint which is doing the load balancing.
 */
class AwsAuthV4 extends Guzzle
{
    protected function _getGuzzleClient(bool $persistent = true): Client
    {
        if (!$persistent || !self::$_guzzleClientConnection) {
            $stack = HandlerStack::create(Utils::chooseHandler());
            $stack->push($this->getConnectTimeoutMiddleware(), 'connect_timeout');
            $stack->push($this->getSigningMiddleware(), 'sign');

            self::$_guzzleClientConnection = new Client([
                'handler' => $stack,
            ]);
        }

        return self::$_guzzleClientConnection;
    }

    protected function getConnectTimeoutMiddleware(): callable
    {
        $connectTimeout = 1;
        if ($this->_connection) {
            $connectTimeout = $this->_connection->getConnectTimeout();
        }

        return static function (callable $handler) use ($connectTimeout): callable {
            return static function (RequestInterface $request, array $options) use ($handler, $connectTimeout) {
                $options[RequestOptions::CONNECT_TIMEOUT] = $connectTimeout;
                return $handler($request, $options);
            };
        };
    }

    protected function _getBaseUrl(Connection $connection): string
    {
        $this->initializePortAndScheme();

        return parent::_getBaseUrl($connection);
    }

    private function getSigningMiddleware(): callable
    {
        $region = $this->getConnection()->hasParam('aws_region')
            ? $this->getConnection()->getParam('aws_region')
            : \getenv('AWS_REGION');
        $signer = new SignatureV4('es', $region);
        $credProvider = $this->getCredentialProvider();

        return Middleware::mapRequest(static function (RequestInterface $req) use (
            $signer,
            $credProvider
        ) {
            return $signer->signRequest($req, $credProvider()->wait());
        });
    }

    private function getCredentialProvider(): callable
    {
        $connection = $this->getConnection();

        if ($connection->hasParam('aws_credential_provider')) {
            return $connection->getParam('aws_credential_provider');
        }

        if ($connection->hasParam('aws_secret_access_key')) {
            return CredentialProvider::fromCredentials(new Credentials(
                $connection->getParam('aws_access_key_id'),
                $connection->getParam('aws_secret_access_key'),
                $connection->hasParam('aws_session_token')
                    ? $connection->getParam('aws_session_token')
                    : null
            ));
        }

        return CredentialProvider::defaultProvider();
    }

    private function initializePortAndScheme(): void
    {
        $connection = $this->getConnection();
        if (true === $this->isSslRequired($connection)) {
            $this->_scheme = 'https';
            $connection->setPort(443);
        } else {
            $this->_scheme = 'http';
            $connection->setPort(80);
        }
    }

    private function isSslRequired(Connection $conn, bool $default = false): bool
    {
        return $conn->hasParam('ssl')
            ? (bool)$conn->getParam('ssl')
            : $default;
    }
}
