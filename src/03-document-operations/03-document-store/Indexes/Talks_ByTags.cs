using System.Linq;
using Raven.Client.Documents.Indexes;
using Sample.Models;

public class Talks_ByTags : AbstractIndexCreationTask<Talk>
{
    public Talks_ByTags()
    {
        Map = talks =>
            from talk in talks
            select new
            {
                talk.Tags
            };
    }
}