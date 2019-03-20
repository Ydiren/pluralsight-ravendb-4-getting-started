using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Sample.Models;
using Raven.Client.Documents;
using System.Linq;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Queries;

namespace Sample.Services
{
    public class RavenTalkService : ITalkService
    {
        private readonly IDocumentStore store;

        public RavenTalkService(IDocumentStoreHolder storeHolder)
        {
            this.store = storeHolder.Store;
        }

        public async Task<Talk> CreateTalk(NewTalk talk)
        {
            using (var session = store.OpenAsyncSession())
            {
                var newTalk = new Talk()
                {
                    Headline = talk.Headline,
                    Description = talk.Description,
                    Speaker = talk.Speaker
                };

                // Store new talk in the cache
                // This will populate the ID of the object passed in
                await session.StoreAsync(newTalk);

                // Commit changes to the actual database
                await session.SaveChangesAsync();

                return newTalk;
            }
        }

        public async Task<TalkDetail> GetTalkDetail(string id)
        {
            using (var session = store.OpenAsyncSession())
            {
                // Include will load the referenced speaker into the cache, ready for the next Load operation
                var talk = await session.Include<Talk>(t => t.Speaker)
                                        .LoadAsync<Talk>(id);

                // This will just fetch the speaker out of the cache, it will
                // not hit the database due to the Include in the previous statement
                var speaker = await session.LoadAsync<Speaker>(talk.Speaker);

                return new TalkDetail()
                {
                    Id = talk.Id,
                    Description = talk.Description,
                    Event = talk.Event,
                    Headline = talk.Headline,
                    Published = talk.Published,
                    Speaker = talk.Speaker,
                    SpeakerName = speaker.Name,
                    SpeakerTalks = new TalkSummary[] { },
                    Tags = talk.Tags
                };
            }
        }

        public async Task<Speaker[]> GetSpeakers()
        {
            using (var session = store.OpenAsyncSession())
            {
                // Load speakers with keys that are prefixed with "Speaker/" (i.e. all of them!)
                var speakers = await session.Advanced.LoadStartingWithAsync<Speaker>("Speakers/", start: 0, pageSize: Constants.PageSize);
                return speakers.ToArray();
            }
        }

        public async Task<Talk> UpdateTalk(string id, UpdatedTalk talk, string version)
        {
            throw new NotImplementedException("TODO: Implement UpdateTalk");
        }

        public async Task<bool> DeleteTalk(string id)
        {
            throw new NotImplementedException("TODO: Implement DeleteTalk");
        }

        public async Task<SpeakerTalkStats[]> GetSpeakerTalkStats()
        {
            throw new NotImplementedException("TODO: Implement GetSpeakerTalkStats");
        }

        public async Task<TagTalkStats[]> GetTagTalkStats()
        {
            throw new NotImplementedException("TODO: Implement GetTagTalkStats");
        }

        public async Task<(UpdatedTalk Talk, string Version)> GetTalkForEditing(string id)
        {
            throw new NotImplementedException("TODO: Implement GetTalkForEditing");
        }

        public async Task<TalkSummary[]> GetTalkSummaries(int page = 1)
        {
            throw new NotImplementedException("TODO: Implement GetTalkSummaries");
        }

        public async Task<TalkSummary[]> GetTalksBySpeaker(string speaker, int show)
        {
            throw new NotImplementedException("TODO: Implement GetTalksBySpeaker");
        }

        public async Task<TalkSummary[]> GetTalksByTag(string tag, int show)
        {
            throw new NotImplementedException("TODO: Implement GetTalksByTag");
        }

        public async Task<TalkSummary[]> SearchTalks(string search, int page = 1)
        {
            throw new NotImplementedException("TODO: Implement SearchTalks");
        }
    }
}