﻿using dexih.repository;
using dexih.transforms;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using static dexih.repository.DexihDatajob;
using static dexih.transforms.TransformWriterResult;
using System.Threading.Tasks.Dataflow;
using Microsoft.Extensions.Logging;

namespace dexih.operations
{
	public class DatajobRun
	{
		#region Events

		public delegate void DatajobProgressUpdate(TransformWriterResult writerResult);
		public event DatajobProgressUpdate OnDatajobProgressUpdate;

		public delegate void DatajobStatusUpdate(TransformWriterResult writerResult);
		public event DatajobStatusUpdate OnDatajobStatusUpdate;

		public delegate void DatalinkStart(DatalinkRun datalinkRun);
		public event DatalinkStart OnDatalinkStart;

		public delegate void Finish(DatajobRun datajobRun);
		public event Finish OnFinish;

		private readonly List<FileSystemWatcher> _fileWatchers = new List<FileSystemWatcher>();

		private readonly BufferBlock<TransformWriterResult> _completedDatalinks = new BufferBlock<TransformWriterResult>();

		#endregion

		public DexihDatajob Datajob { get; set; }
		
		public List<DatalinkRun> DatalinkSteps { get; set; }
		private readonly DexihHub _hub;
		public TransformWriterResult WriterResult { get; private set; }

		private readonly TransformSettings _transformSettings;
		private Connection _auditConnection;
		private readonly TransformWriterOptions _transformWriterOptions;

		private readonly ILogger _logger;


		public DatajobRun(TransformSettings transformSettings, ILogger logger, DexihDatajob datajob, DexihHub hub, TransformWriterOptions transformWriterOptions)
		{
			_transformSettings = transformSettings;
			_logger = logger;

			_transformWriterOptions = transformWriterOptions;

			Connection auditConnection;

			if (datajob.AuditConnectionKey > 0)
			{
				var dbAuditConnection =
					hub.DexihConnections.SingleOrDefault(c => c.ConnectionKey == datajob.AuditConnectionKey);

				if (dbAuditConnection == null)
				{
					throw new DatalinkRunException(
						$"Audit connection with key {datajob.AuditConnectionKey} was not found.");
				}

				auditConnection = dbAuditConnection.GetConnection(_transformSettings);
			}
			else
			{
				auditConnection = new ConnectionMemory();
			}
			
			WriterResult = new TransformWriterResult
			{
				AuditConnection = auditConnection,
				AuditConnectionKey = datajob.AuditConnectionKey ?? 0,
				AuditType = "Datajob",
				HubKey = hub.HubKey,
				ReferenceKey = datajob.DatajobKey,
				ParentAuditKey = 0,
				ReferenceName = datajob.Name,
				SourceTableKey = 0,
				SourceTableName = "",
				TransformWriterOptions = _transformWriterOptions
			};
			
			Datajob = datajob;
			_hub = hub;
		}

		public void ResetEvents()
		{
			OnDatajobProgressUpdate = null;
			OnDatajobStatusUpdate = null;
			OnFinish = null;
			OnDatalinkStart = null;
		}

		public async Task<bool> Initialize(CancellationToken cancellationToken)
		{
			try
			{
				DatalinkSteps = new List<DatalinkRun>();

				if (Datajob.AuditConnectionKey > 0)
				{
					var dbAuditConnection = _hub.DexihConnections.SingleOrDefault(c => c.ConnectionKey == Datajob.AuditConnectionKey);
					if (dbAuditConnection == null)
					{
						throw new DatajobRunException("There is no audit connection specified.");
					}
					_auditConnection = dbAuditConnection.GetConnection(_transformSettings);
				}
				else
				{
					throw new DatajobRunException("There is no audit connection specified.");
				}

				WriterResult.OnProgressUpdate += Datajob_OnProgressUpdate;
				WriterResult.OnStatusUpdate += Datajob_OnStatusUpdate;
				return await WriterResult.Initialize(cancellationToken);
			}
			catch (Exception ex)
			{
				WriterResult.RunStatus = ERunStatus.Abended;
				WriterResult.Message = "Error occurred initializing datajob " + Datajob.Name + ".  " + ex.Message;
				throw new DatajobRunException(WriterResult.Message, ex);
			}
		}

		public void Datajob_OnProgressUpdate(TransformWriterResult writer)
		{
			OnDatajobProgressUpdate?.Invoke(writer);
		}

		public void Datajob_OnStatusUpdate(TransformWriterResult writer)
		{
			OnDatajobStatusUpdate?.Invoke(writer);
			_completedDatalinks.Post(writer);
		}

		public void Schedule(DateTime scheduledTime, CancellationToken cancellationToken)
		{
			var runStatus = WriterResult.SetRunStatus(ERunStatus.Scheduled, null, null, cancellationToken);
			if (!runStatus)
			{
				throw new DatajobRunException($"Failed to set status");
			}

			WriterResult.ScheduledTime = scheduledTime;
		}

		public void CancelSchedule(CancellationToken cancellationToken)
		{
			var runstatusResult = WriterResult.SetRunStatus(ERunStatus.Cancelled, null, null, cancellationToken);
			if (!runstatusResult)
			{
				throw new DatajobRunException($"Failed to set status");
			}

			WriterResult.ScheduledTime = null;
		}

		public async Task<bool> Run(CancellationToken cancellationToken)
		{
			try
			{
				cancellationToken.ThrowIfCancellationRequested();

				WriterResult.StartTime = DateTime.Now;
				WriterResult.LastUpdateTime = DateTime.Now;

				var runStatus = WriterResult.SetRunStatus(ERunStatus.Started, null, null, cancellationToken);
				if (!runStatus)
				{
					throw new DatajobRunException($"Failed to set status");
				}

				//start all jobs async
				foreach (var step in Datajob.DexihDatalinkSteps)
				{
					var datalink = _hub.DexihDatalinks.Single(c => c.DatalinkKey == step.DatalinkKey);
					var datalinkRun = new DatalinkRun(_transformSettings, _logger, Datajob.DatajobKey, datalink, _hub, null, _transformWriterOptions);

					DatalinkSteps.Add(datalinkRun);

					//start datalinks that have no dependencies.
					if (step.DexihDatalinkDependencies == null || step.DexihDatalinkDependencies.Count == 0)
					{
						datalinkRun.OnStatusUpdate += DatalinkStatus;
						datalinkRun.OnFinish += DatalinkStatus;

						//raise an event to have the datalink started.
						if (OnDatalinkStart != null)
						{
							OnDatalinkStart.Invoke(datalinkRun);
						}
						else
						{
							throw new DatajobRunException($"Could not start the datalink {datalink.Name} due to a missing start event.");
						}
					}
				}

				WriterResult.SetRunStatus(ERunStatus.Running, null, null, cancellationToken);

				//wait until all other datalinks have finished
				while (WriterResult.RunStatus != ERunStatus.Finished &&
				       WriterResult.RunStatus != ERunStatus.FinishedErrors &&
				       WriterResult.RunStatus != ERunStatus.Cancelled &&
				       WriterResult.RunStatus != ERunStatus.Abended
				)
				{
					var writer = await _completedDatalinks.ReceiveAsync(cancellationToken);
				}

				await WriterResult.CompleteDatabaseWrites();

				return true;
			}
			catch (OperationCanceledException)
			{
				WriterResult.SetRunStatus(ERunStatus.Cancelled, "Datajob was cancelled", null, cancellationToken);
				throw new DatajobRunException($"The datajob {Datajob.Name} was cancelled.");

			}
			catch (Exception ex)
			{
				var message = $"The job {Datajob.Name} failed.  ${ex.Message}";
				if (WriterResult != null)
				{
					WriterResult?.SetRunStatus(ERunStatus.Abended, message, null, cancellationToken);
				}
				throw new DatajobRunException(message, ex);
			}
			finally
			{
				OnFinish?.Invoke(this);
			}
		}

		/// <summary>
		/// Called whenever a datalink reports an update, and determines when the job should finish or start other datalinks.
		/// </summary>
		public void DatalinkStatus(TransformWriterResult writerResult)
		{
			if (writerResult.RunStatus == ERunStatus.Finished || writerResult.RunStatus == ERunStatus.Abended || writerResult.RunStatus == ERunStatus.Cancelled)
			{
				WriterResult.RowsCreated += writerResult.RowsCreated;
				WriterResult.RowsUpdated += writerResult.RowsUpdated;
				WriterResult.RowsDeleted += writerResult.RowsDeleted;
				WriterResult.RowsFiltered += writerResult.RowsFiltered;
				WriterResult.RowsIgnored += writerResult.RowsIgnored;
				WriterResult.RowsPreserved += writerResult.RowsPreserved;
				WriterResult.RowsRejected += writerResult.RowsRejected;
				WriterResult.RowsReadPrimary += writerResult.RowsReadPrimary;
				WriterResult.RowsReadReference += writerResult.RowsReadReference;
				WriterResult.RowsTotal += writerResult.RowsTotal;

				WriterResult.ReadTicks += writerResult.ReadTicks;
				WriterResult.WriteTicks += writerResult.WriteTicks;
				WriterResult.ProcessingTicks = writerResult.ProcessingTicks;

				OnDatajobProgressUpdate?.Invoke(WriterResult);

				//if the datalink failed, and the job is set to abend on fail, then cancel the other datalinks.
				if (Datajob.FailAction == EFailAction.Abend && (writerResult.RunStatus == ERunStatus.Abended || writerResult.RunStatus == ERunStatus.Cancelled || writerResult.RunStatus == ERunStatus.FinishedErrors))
				{
					WriterResult.SetRunStatus(ERunStatus.Abended, "The job abended due to the datalink " + writerResult.ReferenceName + " abending.", null, CancellationToken.None);
					//Cancel();
					return;
				}
				else if (writerResult.RunStatus == ERunStatus.Finished || Datajob.FailAction == EFailAction.Continue)
				{
					//see if any of the pending jobs, should be started or abended
					foreach (var datalinkStep in DatalinkSteps.Where(c => c.WriterTarget.WriterResult == null || c.WriterTarget.WriterResult.RunStatus == ERunStatus.Initialised))
					{
						var dbStep = Datajob.DexihDatalinkSteps.SingleOrDefault(c => c.DatalinkKey == datalinkStep.Datalink.DatalinkKey);
						if (dbStep.DexihDatalinkDependencies.Any(c => c.DatalinkStepKey == writerResult.ReferenceKey))
						{
							//check if the jobs other dependencies have finished
							var allFinished = true;
							foreach (var dbDependentStep in dbStep.DexihDatalinkDependencies.Where(c => c.DatalinkStepKey != writerResult.ReferenceKey))
							{
								var dependentStep = DatalinkSteps.SingleOrDefault(c => c.Datalink.DatalinkKey == dbDependentStep.DatalinkStep.DatalinkKey);

								if (dependentStep.WriterTarget.WriterResult.RunStatus != ERunStatus.Finished)
								{
									allFinished = false;
									break;
								}
							}
							if (allFinished) //all dependent jobs finished, then run the datalink.
							{
								datalinkStep.OnStatusUpdate += DatalinkStatus;
								datalinkStep.OnFinish += DatalinkStatus;

								//raise an event to have the datalink started.
								if (OnDatalinkStart != null)
									OnDatalinkStart(datalinkStep);
								else
								{
									WriterResult.SetRunStatus(ERunStatus.Abended, "Could not start the datalink due to a missing start event.", null, CancellationToken.None);
									return;
								}
							}
							break;
						}
					}
				}
			}

			//get an overall status
			var runningJobs = false;
			var finished = true;
			foreach (var step in DatalinkSteps)
			{
				var result = step.WriterTarget.WriterResult;
				if (result != null) //this can sometimes be null as some datalinks have progressed whilst others are still inializing.
				{
					switch (result.RunStatus)
					{
						case ERunStatus.Cancelled:
							WriterResult.SetRunStatus(ERunStatus.Cancelled, null, null, CancellationToken.None);
							break;
						case ERunStatus.Abended:
							WriterResult.SetRunStatus(ERunStatus.RunningErrors, null, null, CancellationToken.None);
							break;
						case ERunStatus.Started:
						case ERunStatus.Initialised:
							finished = false;
							runningJobs = true;
							break;
						case ERunStatus.Running:
						case ERunStatus.RunningErrors:
							runningJobs = true;
							finished = false;
							break;
						case ERunStatus.Finished:
						case ERunStatus.FinishedErrors:
						case ERunStatus.NotRunning:
							break;
						default:
							throw new ArgumentOutOfRangeException();
					}
				}
			}

			if (finished)
			{
				if (WriterResult.RunStatus == ERunStatus.RunningErrors)
					WriterResult.SetRunStatus(ERunStatus.FinishedErrors, null, null, CancellationToken.None);
				else if (WriterResult.RunStatus == ERunStatus.Cancelled)
					WriterResult.SetRunStatus(ERunStatus.Cancelled, null, null, CancellationToken.None);
				else
					WriterResult.SetRunStatus(ERunStatus.Finished, null, null, CancellationToken.None);
			}
			else if (runningJobs == false)
			{
				WriterResult.SetRunStatus(ERunStatus.Abended, "Datalinks could not be started, so the job has abended.  This may be due to the dependencies on the datalinks.", null, CancellationToken.None);
			}

			OnDatajobProgressUpdate?.Invoke(WriterResult);
		}
	}
}
