using System;
using System.Collections.Generic;
using System.Linq;
using Akka.Actor;

namespace ActorAggregate
{
    public class Program
    {

        public static void Main(string[] args)
        {
            var system = ActorSystem.Create("System");
            var commandHandlerActor = system.ActorOf(Props.Create(()=> new CommandHandlerActor()), "commandhandler");
            var models = new List<DomainModel>();
            var model = new DomainModel
            {
                Id = Guid.NewGuid(),
                Person = new Person
                {
                    FirstName = "Steve",
                    LastName = "Danger",
                    ZipCode = "60607"
                }
            };
            for (var i = 0; i < 100; i++)
            {
                models.Add(model);
            }

            foreach (var m in models)
            {
                var createPerson = new CreatePerson(m);
                commandHandlerActor.Tell(createPerson);
            }

            Console.Read();
        }
    }
    #region Actors
    public class CommandHandlerActor : ReceiveActor
    {
        public CommandHandlerActor()
        {
            var aggregateActor = Context.ActorOf(Props.Create(() => new AggregateRootActor()), "AggregateActor");
            Receive<CreatePerson>(x =>
            {   
                var personCreatedEvent = new PersonCreated(x.Model, Guid.NewGuid());
                aggregateActor.Tell(personCreatedEvent, Self);
            });
            Receive<ChangeAddress>(x =>
            {
                var personMovedEvent = new PersonMoved(x.ZipCode, x.AggregateId, x.ModelId);
                aggregateActor.Tell(personMovedEvent, Self);
            });
            Receive<ChangeName>(x =>
            {
                var personRenamedEvent = new PersonRenamed(x.FirstName, x.LastName, x.AggregateId, x.ModelId);
                aggregateActor.Tell(personRenamedEvent, Self);
            });
            Receive<RebuildAggregate>(x =>
            {
                var aggregateRebuildEvent = new AggregateRebuildInitiated(x.AggregateId);
                aggregateActor.Tell(aggregateRebuildEvent, Self);
            });
        }
    }

    public class AggregateChangeStoreActor : ReceiveActor
    {
        public AggregateChangeStoreActor()
        {
            var persistenceActor = Context.ActorOf(Props.Create(() => new ReadStoreHydrationActor()), "persistenceActor");
            Receive<PersonCreated>(x =>
            {
                var lastVersion = EventStore.GetLastVersion(x.AggregateId);
                lastVersion++;
                var descriptor = new EventDescriptor(lastVersion, x, x.AggregateId, x.GetType());
                EventStore.AddEvent(descriptor);
                persistenceActor.Tell(new EventPersisted(x.GetType(), x, x.AggregateId));
            });
            Receive<PersonRenamed>(x =>
            {
                var lastVersion = EventStore.GetLastVersion(x.AggregateId);
                lastVersion++;
                var descriptor = new EventDescriptor(lastVersion, x, x.AggregateId, x.GetType());
                EventStore.AddEvent(descriptor);
                persistenceActor.Tell(new EventPersisted(x.GetType(), x, x.AggregateId)); ;
            });

            Receive<PersonMoved>(x =>
            {
                var lastVersion = EventStore.GetLastVersion(x.AggregateId);
                lastVersion++;
                var descriptor = new EventDescriptor(lastVersion, x, x.AggregateId, x.GetType());
                EventStore.AddEvent(descriptor);
                persistenceActor.Tell(new EventPersisted(x.GetType(), x, x.AggregateId));
            });
        }
    }

    public class AggregateRootActor : ReceiveActor
    {
        public AggregateRoot AggregateRoot { get; private set; }
        public AggregateRootActor()
        {
            var changeActor = Context.ActorOf(Props.Create(()=> new AggregateChangeStoreActor()), "ChangeStoreActor");
            Receive<PersonCreated>(x =>
            {
                AggregateRoot = new AggregateRoot(x.AggregateId, x.Model);
                AggregateRoot.Apply(x);
                WriteStore.Save(AggregateRoot);
                changeActor.Tell(x, Self);
            }); 
            Receive<PersonRenamed>(x =>
            {
                AggregateRoot = WriteStore.Get(x.AggregateId);
                AggregateRoot.Apply(x); 
                WriteStore.Save(AggregateRoot);
                changeActor.Tell(x, Self);
            });
            Receive<PersonMoved>(x =>
            {
                AggregateRoot = WriteStore.Get(x.AggregateId);
                AggregateRoot.Apply(x);
                WriteStore.Save(AggregateRoot);
                changeActor.Tell(x, Self);
            });
            Receive<AggregateRebuildInitiated>(x =>
            {
                var events = EventStore.GetEvents(x.AggregateId);
                AggregateRoot = WriteStore.Get(x.AggregateId);
                foreach (var @event in events.OrderBy(t=>t.Version))
                {
                    if (@event.Type == typeof(PersonCreated))
                    {
                        var e = (PersonCreated) @event.Body;
                        AggregateRoot.Apply(e);
                        WriteStore.Save(AggregateRoot);
                        changeActor.Tell(e);
                    }
                    else if (@event.Type == typeof(PersonRenamed))
                    {
                        var e = (PersonRenamed) @event.Body;
                        AggregateRoot.Apply(e);
                        WriteStore.Save(AggregateRoot);
                        changeActor.Tell(e);
                    }
                    else if (@event.Type == typeof(PersonMoved))
                    {
                        var e = (PersonMoved) @event.Body;
                        AggregateRoot.Apply(e);
                        WriteStore.Save(AggregateRoot);
                        changeActor.Tell(e);
                    }

                }
            });
        }
    }

    public class ReadStoreHydrationActor : ReceiveActor
    {
        public ReadStoreHydrationActor()
        {
            Receive<EventPersisted>(x =>
            {
                if (x.EventType == typeof(PersonCreated))
                {
                    var @event = (PersonCreated) x.EventBody;
                    ReadStore.Handle(@event);
                }
                else if (x.EventType == typeof(PersonMoved))
                {
                    var @event = (PersonMoved) x.EventBody;
                    ReadStore.Handle(@event);
                }
                else if (x.EventType == typeof(PersonRenamed))
                {
                    var @event = (PersonRenamed) x.EventBody;
                    ReadStore.Handle(@event);
                }
            });
        }
    }
    
    #endregion

    #region Events
    public abstract class Event
    {
        public Guid AggregateId { get; }
        public int Version { get; set; }

        protected Event(Guid aggregateId)
        {
            AggregateId = aggregateId;
        }
    }

    public class AggregateRebuildInitiated : Event
    {
        public AggregateRebuildInitiated(Guid aggregateId) : base(aggregateId)
        {
            
        }
    }
    public class PersonCreated : Event
    {
        public DomainModel Model { get; }

        public PersonCreated(DomainModel model, Guid aggregateId) : base(aggregateId)
        {
            Model = model;
        }
    }
    public class EventPersisted : Event
    {
        public Type EventType { get; }
        public Event EventBody { get; }
        public EventPersisted(Type eventType, Event eventBody, Guid aggregateId) : base(aggregateId)
        {
            EventType = eventType;
            EventBody = eventBody;
        }
    }
    public class PersonRenamed : Event
    {
        public string FirstName { get; } 
        public string LastName { get; }
        public Guid ModelId { get; }
        public PersonRenamed(string firstName, string lastName, Guid aggregateId, Guid modelId) : base(aggregateId)
        {
            FirstName = firstName;
            LastName = lastName;
            ModelId = modelId;
        }
    }
    public class PersonMoved : Event
    {
        public string ZipCode { get; }
        public Guid ModelId { get; }
        public PersonMoved(string zipCode, Guid aggregateId, Guid modelId) : base(aggregateId)
        {
            ZipCode = zipCode;
            ModelId = modelId;
        }
    }
    #endregion

    #region Commands

    public class Command
    {
        
    }

    public class RebuildAggregate : Command
    {
        public Guid AggregateId { get; }

        public RebuildAggregate(Guid aggregateId)
        {
            AggregateId = aggregateId;
        }
    }

    public class CreatePerson : Command
    {
        public DomainModel Model { get; }

        public CreatePerson(DomainModel model)
        {
            Model = model;
        }
    }
    public class ChangeAddress : Command
    {
        public string ZipCode { get; }
        public Guid AggregateId { get; }
        public Guid ModelId { get; }
        public ChangeAddress(string zipCode, Guid aggregateId, Guid modelId)
        {
            ZipCode = zipCode;
            AggregateId = aggregateId;
            ModelId = modelId;
        }
    }

    public class ChangeName : Command
    {
        public string FirstName { get; }
        public string LastName { get; }
        public Guid AggregateId { get; }
        public Guid ModelId { get; }
        public ChangeName(string firstName, string lastName, Guid aggregateId, Guid modelId)
        {
            FirstName = firstName;
            LastName = lastName;
            AggregateId = aggregateId;
            ModelId = modelId;
        }
    }
    #endregion

    #region DAL
    public class Person
    {
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string ZipCode { get; set; }
    }

    public static class WriteStore
    {
        private static List<AggregateRoot> Store => new List<AggregateRoot>();

        public static AggregateRoot GetByModelId(Guid id)
        {
            var aggregate = Store.FirstOrDefault(x => x.DomainModel.Id == id);
            if (aggregate == null)
            {
                throw new Exception("No Aggregate Found");
            }
            return aggregate;
        }
        public static AggregateRoot Get(Guid id)
        {
            var aggregate =  Store.FirstOrDefault(x => x.Id == id);
            if (aggregate == null)
            {
                throw new Exception("No Aggregate Found");
            }
            return aggregate;
        }
        public static void Save(AggregateRoot model)
        {
            Store.Add(model);
        }

        
    }

    public static class ReadStore
    {
        private static List<DomainModel> Store => new List<DomainModel>();

        public static DomainModel Get(Guid id)
        {
            return Store.FirstOrDefault(x => x.Id == id);
        }
        public static void Handle(PersonCreated @event)
        {
            Store.Add(@event.Model);

            var model = @event.Model;
            Console.WriteLine("Person Saved to ReadStore: Id {0}, First Name {1}, Last Name {2}, Zip Code {3}", model.Id, model.Person.FirstName, model.Person.LastName, model.Person.ZipCode);
        }
        public static void Handle(PersonMoved @event)
        {
            var model = Store.FirstOrDefault(x => x.Id == @event.ModelId);
            if (model == null)
            {
                throw new Exception("No model found for that Id");
            }
            model.Person.ZipCode = @event.ZipCode;
        }

        public static void Handle(PersonRenamed @event)
        {
            var model = Store.FirstOrDefault(x => x.Id == @event.ModelId);
            if (model == null)
            {
                throw new Exception("No Aggregate Found for that Id");
            }
            model.Person.FirstName = @event.FirstName;
            model.Person.LastName = @event.LastName;
        }
    }
    public class EventDescriptor
    {
        public int Version { get; }
        public Event Body { get; }
        public Type Type { get; }
        public Guid AggregateId { get; }

        public EventDescriptor(int version, Event body, Guid aggregateId, Type type)
        {
            Version = version;
            Body = body;
            AggregateId = aggregateId;
            Type = type;
        }

    }
    public static class EventStore
    {
        private static List<EventDescriptor> Events => new List<EventDescriptor>();
        public static void AddEvent(EventDescriptor @event)
        {
            Events.Add(@event);
        }
        public static List<EventDescriptor> GetEvents(Guid aggregateId)
        {
            return Events.Where(x=>x.AggregateId == aggregateId).ToList();
        }

        public static int GetLastVersion(Guid aggregateId)
        {
            return Events.Any() ? Events.Max(x => x.Version) : 0;
        }
    }
   
    #endregion

    #region Aggregates
    public class DomainModel
    {
        public Guid Id { get; set; }
        public Person Person { get; set; }
    }
    public class AggregateRoot
    {
        public Guid Id { get; }
        public DomainModel DomainModel { get; private set; }

        public void Apply(PersonCreated @event)
        {
            DomainModel = @event.Model;
        }

        public void Apply(PersonRenamed @event)
        {
            DomainModel.Person.FirstName = @event.FirstName;
            DomainModel.Person.LastName = @event.LastName;
        }

        public void Apply(PersonMoved @event)
        {
            DomainModel.Person.ZipCode = @event.ZipCode;
        }

        public AggregateRoot(Guid id, DomainModel model)
        {
            Id = id;
            DomainModel = model;
        }
    }

    #endregion
}
