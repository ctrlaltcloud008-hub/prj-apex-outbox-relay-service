package pubsub

import (
	"context"
	"sync"

	"cloud.google.com/go/pubsub/v2"

	"github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/outbox"
	pbclient "github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/pubsub"
)

type TopicPublisher struct {
	client     *pubsub.Client
	publishers sync.Map
}

func NewTopicPublisher(client *pubsub.Client) *TopicPublisher {
	return &TopicPublisher{
		client: client,
	}
}

func (p *TopicPublisher) Publish(ctx context.Context, topicName string, data []byte, attrs map[string]string) error {
	publisher := p.getOrCreatePublisher(topicName)

	result := publisher.Publish(ctx, &pubsub.Message{
		Data:       data,
		Attributes: attrs,
	})

	_, err := result.Get(ctx)
	return err
}

func (p *TopicPublisher) PublishFromOutbox(ctx context.Context, topicName string, env outbox.Envelope, data []byte, attrs map[string]string) *pubsub.PublishResult {
	publisher := p.getOrCreatePublisher(topicName)

	return publisher.PublishFromOutbox(ctx, env, &pubsub.Message{
		Data:       data,
		Attributes: attrs,
	})
}

func (p *TopicPublisher) getOrCreatePublisher(topicName string) *pbclient.Publisher {
	if publisher, ok := p.publishers.Load(topicName); ok {
		return publisher.(*pbclient.Publisher)
	}

	publisher := pbclient.NewPublisher(p.client, topicName,
		pbclient.WithCountThreshold(100),
		pbclient.WithDelayThreshold(10),
		pbclient.WithByteThreshold(1*1024*1024))

	actual, _ := p.publishers.LoadOrStore(topicName, publisher)

	return actual.(*pbclient.Publisher)
}

func (p *TopicPublisher) Stop() {
	p.publishers.Range(func(_, value any) bool {
		value.(*pbclient.Publisher).Stop()
		return true
	})
}
