using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Sample.Models;
using Raven.Client.Documents;
using System.Linq;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;

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
            using (var session = store.OpenAsyncSession())
            {
                var existingTalk = await session.LoadAsync<Talk>(id);

                existingTalk.Headline = talk.Headline;
                existingTalk.Description = talk.Description;
                existingTalk.Speaker = talk.Speaker;

                try
                {
                    await session.StoreAsync(existingTalk, version, id);
                    await session.SaveChangesAsync();
                }
                catch (ConcurrencyException cex)
                {
                    throw new ApplicationException("Tried to update a talk but it looks like someone else got there first! " +
                    "Try refreshing the page. Detailed explanation: " + cex.Message, cex);
                }

                return existingTalk;
            }
        }

        public async Task<bool> DeleteTalk(string id)
        {
            using (var session = store.OpenAsyncSession())
            {
                session.Delete(id);
                await session.SaveChangesAsync();
                return true;
            }
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
            using (var session = store.OpenAsyncSession())
            {
                var talk = await session.LoadAsync<Talk>(id);
                var version = session.Advanced.GetChangeVectorFor(talk);

                var updatedTalk = new UpdatedTalk()
                {
                    Description = talk.Description,
                    Headline = talk.Headline,
                    Speaker = talk.Speaker
                };

                return (Talk: updatedTalk, Version: version);
            }
        }

        public async Task<TalkSummary[]> GetTalkSummaries(int page = 1)
        {
            using (var session = store.OpenAsyncSession())
            {
                var actualPage = Math.Max(0, page - 1);

                var talks = await session.Query<Talk>()
                                        .Skip(actualPage * Constants.PageSize)
                                        .Take(Constants.PageSize)
                                        .Select(talk => new TalkSummary()
                                        {
                                            Id = talk.Id,
                                            Headline = talk.Headline,
                                            Description = talk.Description,
                                            Published = talk.Published,
                                            Speaker = talk.Speaker,
                                            SpeakerName = RavenQuery.Load<Speaker>(talk.Speaker).Name
                                        })
                                        .ToListAsync();
                return talks.ToArray();
            }
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