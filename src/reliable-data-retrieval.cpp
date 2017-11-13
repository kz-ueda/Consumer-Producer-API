/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */
/**
 * Copyright (c) 2014-2016 Regents of the University of California.
 *
 * This file is part of Consumer/Producer API library.
 *
 * Consumer/Producer API library library is free software: you can redistribute it and/or 
 * modify it under the terms of the GNU Lesser General Public License as published by the Free 
 * Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * Consumer/Producer API library is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 *
 * You should have received copies of the GNU General Public License and GNU Lesser
 * General Public License along with Consumer/Producer API, e.g., in COPYING.md file.  If not, see
 * <http://www.gnu.org/licenses/>.
 *
 * See AUTHORS.md for complete list of Consumer/Producer API authors and contributors.
 */

#include "reliable-data-retrieval.hpp"
#include "consumer-context.hpp"

namespace ndn {

ReliableDataRetrieval::ReliableDataRetrieval(Context* context,
                                            const ExtendedRdrOptions& options)
  : DataRetrievalProtocol(context)
  , m_isFinalBlockNumberDiscovered(false)
  , m_finalBlockNumber(std::numeric_limits<uint64_t>::max())
  , m_lastReassembledSegment(0)
  , m_contentBufferSize(0)
  , m_currentWindowSize(0)
  , m_interestsInFlight(0)
  , m_segNumber(0)
  , m_options(options)
  , m_isPacing(false)
  , m_isLogging(false)
  , m_minRTT(0)
  , m_maxRTT(0)
  , m_ssthresh(m_options.initSsthresh)
{
  context->getContextOption(FACE_CONFIG, m_face);
  m_scheduler = new Scheduler(m_face->getIoService());
  m_context->getContextOption(LOGGING, isLogging);
}

ReliableDataRetrieval::~ReliableDataRetrieval()
{
  stop();
  delete m_scheduler;
}

void
ReliableDataRetrieval::start()
{
  m_isRunning = true;
  m_isFinalBlockNumberDiscovered = false;
  m_finalBlockNumber = std::numeric_limits<uint64_t>::max();
  m_segNumber = 0;
  m_interestsInFlight = 0;
  m_lastReassembledSegment = 0;
  m_contentBufferSize = 0;
  m_contentBuffer.clear();
  m_interestRetransmissions.clear();
  m_receiveBuffer.clear();
  m_unverifiedSegments.clear();
  m_verifiedManifests.clear();
  m_startTime = time::steady_clock::now();
  
  // Inport finalBlockNumber from context
  int finalBlockFromContext = -1;
  m_context->getContextOption(FINAL_BLOCK_NUMBER, finalBlockFromContext);
  if (finalBlockFromContext > 0)
  {
    m_finalBlockNumber = finalBlockFromContext;
    m_isFinalBlockNumberDiscovered = true;
  }  

  // this is to support window size "inheritance" between consume calls
  int currentWindowSize = -1;
  m_context->getContextOption(CURRENT_WINDOW_SIZE, currentWindowSize);
  if (currentWindowSize > 0)
  {
    m_currentWindowSize = currentWindowSize;
  }
  else
  {
    int minWindowSize = -1;
    m_context->getContextOption(MIN_WINDOW_SIZE, minWindowSize);
    m_currentWindowSize = minWindowSize;
  }

  // RWIN has already configured -> controlOutgoingInterests
  // Default: inflight==0, currentRWIN == -1
  if (m_interestsInFlight < m_currentWindowSize)
  {
    controlOutgoingInterests();
  }
  else
  {
    //send exactly 1 Interest to get the FinalBlockId
    sendInterest();
  }

  bool isAsync = false;
  m_context->getContextOption(ASYNC_MODE, isAsync);
  bool isContextRunning = false;
  m_context->getContextOption(RUNNING, isContextRunning);
  if (!isAsync && !isContextRunning)
  {
    m_context->setContextOption(RUNNING, true);
    m_face->processEvents();
  }
}

void
ReliableDataRetrieval::sendInterest()
{
  Name prefix;
  m_context->getContextOption(PREFIX, prefix);
  
  Name suffix;
  m_context->getContextOption(SUFFIX, suffix);
  
  if (!suffix.empty())
  {
    prefix.append(suffix);
  }
  
  prefix.appendSegment(m_segNumber);

  Interest interest(prefix);
  
  int interestLifetime = DEFAULT_INTEREST_LIFETIME;
  m_context->getContextOption(INTEREST_LIFETIME, interestLifetime);
  interest.setInterestLifetime(time::milliseconds(interestLifetime));
  
  SelectorHelper::applySelectors(interest, m_context);
  
  ConsumerInterestCallback onInterestToLeaveContext = EMPTY_CALLBACK;
  m_context->getContextOption(INTEREST_LEAVE_CNTX, onInterestToLeaveContext);
  if (onInterestToLeaveContext != EMPTY_CALLBACK)
  {
    onInterestToLeaveContext(*dynamic_cast<Consumer*>(m_context), interest);
  }
  
  // because user could stop the context in one of the prev callbacks
  //if (m_isRunning == false)
  //  return;
  
  m_interestsInFlight++;
  if(m_isLogging)
  {
    std::cout << ndn::time::toUnixTimestamp(time::system_clock::now()).count() << ", " << time::steady_clock::now() - getStartTime()
    << " RDR::sendInterest::inflight = " << m_interestsInFlight << ", windowSize = " << m_currentWindowSize << ", name = " << interest.getName().toUri() << std::endl; 
  }
  m_interestRetransmissions[m_segNumber] = 0;
  m_interestTimepoints[m_segNumber] = time::steady_clock::now();
  m_expressedInterests[m_segNumber] = m_face->expressInterest(interest,
                                                bind(&ReliableDataRetrieval::onData, this, _1, _2),
                                                bind(&ReliableDataRetrieval::onTimeout, this, _1));
  m_scheduledInterests.erase(m_segNumber);
  m_segNumber++;
}

void
ReliableDataRetrieval::stop()
{
  m_isRunning = false;
  removeAllPendingInterests();
  removeAllScheduledInterests();
}

void
ReliableDataRetrieval::getNetworkStatistics(double minRTT, double maxRTT, int currentWindow)
{
  minRTT = m_minRTT;
  maxRTT = m_maxRTT;
  currentWindow = m_currentWindowSize;
}

void
ReliableDataRetrieval::onData(const ndn::Interest& interest, ndn::Data& data)
{
  if (m_isRunning == false)
    return;

  m_interestsInFlight--;

  uint64_t segment = interest.getName().get(-1).toSegment();
  m_expressedInterests.erase(segment);

  if (m_interestTimepoints.find(segment) != m_interestTimepoints.end())
  {
    time::steady_clock::duration duration = time::steady_clock::now() - m_interestTimepoints[segment];
    if(m_isLogging)
    {
      std::cout << ndn::time::toUnixTimestamp(time::system_clock::now()).count() << ", " << time::steady_clock::now() - getStartTime()
      << " RDR::onData::RTT = " << duration << ", name = " << data.getName().toUri() << std::endl; 
    }
    m_rttEstimator.addMeasurement(boost::chrono::duration_cast<boost::chrono::microseconds>(duration));
    // Update min/max RTT
    double m = static_cast<double>(duration.count());
    if (m_minRTT == 0 || m_minRTT > m)
      m_minRTT = m;
    if (m_maxRTT < m)
      m_maxRTT = m; 
    
    RttEstimator::Duration rto = m_rttEstimator.computeRto();
    boost::chrono::milliseconds lifetime = boost::chrono::duration_cast<boost::chrono::milliseconds>(rto);
    
    int interestLifetime = DEFAULT_INTEREST_LIFETIME;
    m_context->getContextOption(INTEREST_LIFETIME, interestLifetime);
    
    // update lifetime only if user didn't specify prefered value
    if (interestLifetime == DEFAULT_INTEREST_LIFETIME)  
      m_context->setContextOption(INTEREST_LIFETIME, (int)lifetime.count());
  }

  ConsumerDataCallback onDataEnteredContext = EMPTY_CALLBACK;
  m_context->getContextOption(DATA_ENTER_CNTX, onDataEnteredContext);
  if (onDataEnteredContext != EMPTY_CALLBACK)
    onDataEnteredContext(*dynamic_cast<Consumer*>(m_context), data);
  
  ConsumerInterestCallback onInterestSatisfied = EMPTY_CALLBACK;
  m_context->getContextOption(INTEREST_SATISFIED, onInterestSatisfied);
  if (onInterestSatisfied != EMPTY_CALLBACK)
    onInterestSatisfied(*dynamic_cast<Consumer*>(m_context), const_cast<Interest&>(interest));

  if (data.getContentType() == MANIFEST_DATA_TYPE)
    onManifestData(interest, data);
  else if (data.getContentType() == NACK_DATA_TYPE)
    onNackData(interest, data);
  else if (data.getContentType() == CONTENT_DATA_TYPE)
    onContentData(interest, data);
  
  if (m_isRunning){
    // SENDING NEXT INTERESTS: Control RWIN -> Schedule outgoing interests
    // 1. Control RWIN
    //    おそらくonContentData()で既に制御済み
    //    SegmentFetcherの場合、ここでfinalBlockIDと等しくしていた
    // 2. Schedule outgoing interests
    //    Current: pacing or not
    controlOutgoingInterests();
  }
}

void
ReliableDataRetrieval::increaseWindow()
{
  int maxWindowSize = -1;
  m_context->getContextOption(MAX_WINDOW_SIZE, maxWindowSize);
  int flowControlProtocol = 1;
  m_context->getContextOption(FLOW_CONTROL, flowControlProtocol);
  // [1] Segment fetcher, [2] AIMD, [3] VEGAS 
  switch(flowControlProtocol)
  {
    case 1:
      //SegmentFetcher();
      // In responce to the first Interest, try to transmit all Interests, except the first one as a next round 
      if (m_isFinalBlockNumberDiscovered)
        m_currentWindowSize = m_finalBlockNumber; 
      // if there are too many Interests to send, put an upper boundary on it.
      if (m_currentWindowSize > maxWindowSize) 
        m_currentWindowSize = maxWindowSize;
      break;

    case 2:
      // AIMD
      if (m_currentWindowSize < m_ssthresh){
        m_currentWindowSize += m_options.aiStep; //additive increase
      }
      else{
        // congestion avoidance
        m_currentWindowSize += m_options.aiStep / std::floor(m_currentWindowSize);
      }
      break;

    case 3:
      // VEGAS
      break;

    default:
      // default increase method
      if (m_currentWindowSize < maxWindowSize) // don't expand window above max level
      {
        m_currentWindowSize++;
      }    
      break;
  }
  // After increasing window size:
  m_context->setContextOption(CURRENT_WINDOW_SIZE, m_currentWindowSize);
  //afterCwndChange(time::steady_clock::now() - getStartTime(), m_currentWindowSize);
}
void
ReliableDataRetrieval::decreaseWindow()
{
  int minWindowSize = -1;
  m_context->getContextOption(MIN_WINDOW_SIZE, minWindowSize);
  int flowControlProtocol = 1;
  m_context->getContextOption(FLOW_CONTROL, flowControlProtocol);
  // [1] Segment fetcher, [2] AIMD, [3] VEGAS 
  switch(flowControlProtocol)
  {
    case 1:
      if (m_currentWindowSize > minWindowSize) // don't shrink window below minimum level
      {
        m_currentWindowSize = m_currentWindowSize / 2; // cut in half
        if (m_currentWindowSize == 0)
          m_currentWindowSize++;
        m_context->setContextOption(CURRENT_WINDOW_SIZE, m_currentWindowSize);
      }
      break;
    case 2:
      // AIMD
      // please refer to RFC 5681, Section 3.1 for the rationale behind it
      m_ssthresh = std::max(2.0, m_currentWindowSize * m_options.mdCoef); // multiplicative decrease
      m_currentWindowSize = m_options.resetCwndToInit ? m_options.initCwnd : m_ssthresh;
      break;
    case 3:
      // VEGAS
      break;
    default:
      if (m_currentWindowSize > minWindowSize) // don't shrink window below minimum level
      {
        m_currentWindowSize = m_currentWindowSize / 2; // cut in half
        if (m_currentWindowSize == 0)
          m_currentWindowSize++;
      }
      break;
  }
  // After decreasing window size:
  m_context->setContextOption(CURRENT_WINDOW_SIZE, m_currentWindowSize);
  //afterCwndChange(time::steady_clock::now() - getStartTime(), m_currentWindowSize);
}

void
ReliableDataRetrieval::controlOutgoingInterests()
{
  int pacing = 0;
  m_context->getContextOption(PACING_INTERVAL, pacing);
  if (pacing > 0)
  {
    if(!m_isPacing)
    {
      // totalのinflight数がRWINを超えないか確認. m_segNumber=次に投げるセグメント番号.
      int totalInflight = m_interestsInFlight + m_scheduledInterests.size();
      if (totalInflight < m_currentWindowSize)
      { 
        // inflight availability
        int availability = m_currentWindowSize - totalInflight;
        if (m_isFinalBlockNumberDiscovered)
        {
          if(m_isLogging){
            std::cout << ndn::time::toUnixTimestamp(time::system_clock::now()).count() << ", " << time::steady_clock::now() - getStartTime()
            << " RDR::onData::availability: " << availability << ", totalInflight:" << totalInflight << ", scheduled: " << m_scheduledInterests.size()
            << ", m_segNumber: " << m_segNumber << ", m_finalBlockNumber: " << m_finalBlockNumber << ", m_currentWindowSize: " << m_currentWindowSize << std::endl;
          }
          if(m_segNumber + m_scheduledInterests.size() <= m_finalBlockNumber)
          {
            // Target: sendInterest when (m_segNumber == final)
            // m_segNumber + m_scheduled.size() = nextRequestSegmentNumber
            //  # of interests required for finalBlock = finalBlockNumber - nextRequestSegmentNumber + 1
            // -> availability is bigger than that number??
            if(availability <= m_finalBlockNumber - m_segNumber - m_scheduledInterests.size() + 1)
              paceInterests(availability, time::milliseconds(pacing));
            else
              paceInterests(m_finalBlockNumber - m_segNumber - m_scheduledInterests.size() + 1, time::milliseconds(pacing));
          }
        }
        else
        {
          sendInterest();
        }
      }
    }
  }
  // No pacing
  else
  {
    while (m_interestsInFlight < m_currentWindowSize)
    {
      if (m_isFinalBlockNumberDiscovered)
      {
        if (m_segNumber <= m_finalBlockNumber)
          sendInterest();
        else
          break;
      }
      else
        sendInterest();
    }
  } 
}

void
ReliableDataRetrieval::paceInterests(int nInterests, time::milliseconds timeWindow)
{
  m_isPacing = true;
  if (nInterests <= 0)
    return;
  //time::nanoseconds interval = time::nanoseconds(timeWindow) / nInterests; 
  time::nanoseconds interval = time::nanoseconds(timeWindow); 
  
  if(m_isLogging){
    std::cout << ndn::time::toUnixTimestamp(time::system_clock::now()).count() 
    << " PACE INTEREST FOR " << nInterests << " Interests, Interval:" << interval << std::endl; 
  }
  int nextSegment = m_segNumber + m_scheduledInterests.size();
  for (int i = 0; i < nInterests; i++)
  {
    if(m_isLogging){
      std::cout << "Schedule Interests for " << i*interval << std::endl;
    }
    m_scheduledInterests[nextSegment + i] = m_scheduler->scheduleEvent(i*interval,
                          bind(&ReliableDataRetrieval::sendInterest, this));
  }
  // pacing finished.
  m_isPacing = false; 
}

void
ReliableDataRetrieval::onManifestData(const ndn::Interest& interest, ndn::Data& data)
{
  if (m_isRunning == false)
    return;

  //std::cout << "OnManifest" << std::endl;
  ConsumerDataVerificationCallback onDataToVerify = EMPTY_CALLBACK;
  m_context->getContextOption(DATA_TO_VERIFY, onDataToVerify);
  
  bool isDataSecure = false;

  if (onDataToVerify == EMPTY_CALLBACK)
  {
    // perform integrity check if possible
    if (data.getSignature().getType() == tlv::DigestSha256)
    {
      ndn::name::Component claimedDigest( data.getSignature().getValue().value(), 
                                          data.getSignature().getValue().value_size());
                   
      // recalculate digest
      m_keyChain.signWithSha256(data);
        
      ndn::name::Component actualDigest(data.getSignature().getValue().value(), 
                                        data.getSignature().getValue().value_size());
                                      
      if (!claimedDigest.equals(actualDigest))
      {
        isDataSecure = false;
      }
    }
    else
    {
      isDataSecure = true;
    }
  }
  else
  {
    if (onDataToVerify(*dynamic_cast<Consumer*>(m_context), data) == true) // runs verification routine
    {
      isDataSecure = true;
    }
  }
      
  if (isDataSecure)
  {
    checkFastRetransmissionConditions(interest);
  
    increaseWindow();

    shared_ptr<Manifest> manifest = make_shared<Manifest>(data);
      
    //std::cout << "MANIFEST CONTAINS " << manifest->size() << " names" << std::endl;
      
    m_verifiedManifests.insert(std::pair<uint64_t, shared_ptr<Manifest> >( 
                                                              data.getName().get(-1).toSegment(),
                                                              manifest));
      
    m_receiveBuffer[manifest->getName().get(-1).toSegment()] = manifest;
    
    // TODO: names in manifest are in order, so we can exit the loop earlier
    for(std::map<uint64_t, shared_ptr<Data> >::iterator it = m_unverifiedSegments.begin(); 
                                                        it != m_unverifiedSegments.end(); ++it)
    {
      if (!m_isRunning)
      {
        return;
      }
        
      // data segment is verified with manifest
      if (verifySegmentWithManifest(*manifest, *(it->second))) 
      {
        if (!it->second->getFinalBlockId().empty())
        {
          m_isFinalBlockNumberDiscovered = true;
          m_finalBlockNumber = it->second->getFinalBlockId().toSegment();
        }
          
        m_receiveBuffer[it->second->getName().get(-1).toSegment()] = it->second;
        reassemble();
      }
      else // data segment failed verification with manifest
      {          
        // retransmit interest with implicit digest from the manifest
        retransmitInterestWithDigest(interest, data, *manifest);
      }
    }                          
  }
  else // failed to verify manifest
  {
    retransmitInterestWithExclude(interest,data);
  }
}

void
ReliableDataRetrieval::retransmitFreshInterest(const ndn::Interest& interest)
{
  int maxRetransmissions;
  m_context->getContextOption(INTEREST_RETX, maxRetransmissions);
  
  uint64_t segment = interest.getName().get(-1).toSegment();
  if(m_interestRetransmissions[segment] < maxRetransmissions)
  {
    if (m_isRunning)
    {
      Interest retxInterest(interest.getName());  // because we need new nonce
      int interestLifetime = DEFAULT_INTEREST_LIFETIME;
      m_context->getContextOption(INTEREST_LIFETIME, interestLifetime);
      retxInterest.setInterestLifetime(time::milliseconds(interestLifetime));
            
      SelectorHelper::applySelectors(retxInterest, m_context);
            
      retxInterest.setMustBeFresh(true); // to bypass cache
  
      // this is to inherit the exclusions from the nacked interest
      Exclude exclusion = retxInterest.getExclude();
      for(Exclude::const_iterator it = interest.getExclude().begin();
                                  it != interest.getExclude().end(); it++)
      {
        exclusion.excludeRange(it->from, it->to);
      }
      /* Old version of exclude API
      for(Exclude::exclude_type::const_iterator it = interest.getExclude().begin();
                                              it != interest.getExclude().end(); ++it)
      {
        exclusion.appendExclude(it->first, false);
      }
      */

      retxInterest.setExclude(exclusion);
  
      ConsumerInterestCallback onInterestRetransmitted = EMPTY_CALLBACK;
      m_context->getContextOption(INTEREST_RETRANSMIT, onInterestRetransmitted);
    
      if (onInterestRetransmitted != EMPTY_CALLBACK)
      {
        onInterestRetransmitted(*dynamic_cast<Consumer*>(m_context), retxInterest);
      }
    
      ConsumerInterestCallback onInterestToLeaveContext = EMPTY_CALLBACK;
      m_context->getContextOption(INTEREST_LEAVE_CNTX, onInterestToLeaveContext);
      if (onInterestToLeaveContext != EMPTY_CALLBACK)
      {
        onInterestToLeaveContext(*dynamic_cast<Consumer*>(m_context), retxInterest);
      }
    
      // because user could stop the context in one of the prev callbacks
      if (m_isRunning == false)
        return;
    
      m_interestsInFlight++;
      m_interestRetransmissions[segment]++;
      m_expressedInterests[segment] = m_face->expressInterest(retxInterest,
                                                bind(&ReliableDataRetrieval::onData, this, _1, _2),
                                                bind(&ReliableDataRetrieval::onTimeout, this, _1));
    }
  }
  else
  {
    m_isRunning = false;
  }
}

bool
ReliableDataRetrieval::retransmitInterestWithExclude( const ndn::Interest& interest, 
                                                      Data& dataSegment)
{
  int maxRetransmissions;
  m_context->getContextOption(INTEREST_RETX, maxRetransmissions);
  
  uint64_t segment = interest.getName().get(-1).toSegment();
  m_unverifiedSegments.erase(segment); // remove segment, because it is useless
          
  if(m_interestRetransmissions[segment] < maxRetransmissions)
  {
    Interest interestWithExlusion(interest.getName()); 
    int interestLifetime = DEFAULT_INTEREST_LIFETIME;
    m_context->getContextOption(INTEREST_LIFETIME, interestLifetime);
    interestWithExlusion.setInterestLifetime(time::milliseconds(interestLifetime));
  
    SelectorHelper::applySelectors(interestWithExlusion, m_context);
  
    int nMaxExcludedDigests = 0;
    m_context->getContextOption(MAX_EXCLUDED_DIGESTS, nMaxExcludedDigests);
    
    if (interest.getExclude().size() < (unsigned int) nMaxExcludedDigests)
    {
      const Block& block = dataSegment.wireEncode();
      ndn::ConstBufferPtr implicitDigestBuffer = ndn::crypto::sha256(block.wire(), block.size());
      name::Component implicitDigest = name::Component::fromImplicitSha256Digest(implicitDigestBuffer);
    
      Exclude exclusion = interest.getExclude();
      exclusion.excludeOne(implicitDigest);
      interestWithExlusion.setExclude(exclusion);
    }
    else
    {
      m_isRunning = false;
      return false;
    }
  
    ConsumerInterestCallback onInterestRetransmitted = EMPTY_CALLBACK;
    m_context->getContextOption(INTEREST_RETRANSMIT, onInterestRetransmitted);
    
    if (onInterestRetransmitted != EMPTY_CALLBACK)
    {
      onInterestRetransmitted(*dynamic_cast<Consumer*>(m_context), interestWithExlusion);
    }
    
    ConsumerInterestCallback onInterestToLeaveContext = EMPTY_CALLBACK;
    m_context->getContextOption(INTEREST_LEAVE_CNTX, onInterestToLeaveContext);
    if (onInterestToLeaveContext != EMPTY_CALLBACK)
    {
      onInterestToLeaveContext(*dynamic_cast<Consumer*>(m_context), interestWithExlusion);
    }
    
    // because user could stop the context in one of the prev callbacks
    if (m_isRunning == false)
      return false;
      
    //retransmit
    m_interestsInFlight++;
    m_interestRetransmissions[segment]++;
    m_expressedInterests[segment] = m_face->expressInterest(interestWithExlusion,
                                                bind(&ReliableDataRetrieval::onData, this, _1, _2),
                                                bind(&ReliableDataRetrieval::onTimeout, this, _1));
  }
  else
  {
    m_isRunning = false;
    return false;
  }

  return true;
}

bool
ReliableDataRetrieval::retransmitInterestWithDigest(const ndn::Interest& interest, 
                                                    const Data& dataSegment, 
                                                    Manifest& manifestSegment)
{ 
  int maxRetransmissions;
  m_context->getContextOption(INTEREST_RETX, maxRetransmissions);
  
  uint64_t segment = interest.getName().get(-1).toSegment();
  m_unverifiedSegments.erase(segment); // remove segment, because it is useless
          
  if(m_interestRetransmissions[segment] < maxRetransmissions)
  {
    name::Component implicitDigest = getDigestFromManifest(manifestSegment, dataSegment);
    if (implicitDigest.empty())
    {
      m_isRunning = false;
      return false;
    }
            
    Name nameWithDigest(interest.getName());
    nameWithDigest.append(implicitDigest);
            
    Interest interestWithDigest(nameWithDigest); 
    int interestLifetime = DEFAULT_INTEREST_LIFETIME;
    m_context->getContextOption(INTEREST_LIFETIME, interestLifetime);
    interestWithDigest.setInterestLifetime(time::milliseconds(interestLifetime));
  
    SelectorHelper::applySelectors(interestWithDigest, m_context);
  
    ConsumerInterestCallback onInterestRetransmitted = EMPTY_CALLBACK;
    m_context->getContextOption(INTEREST_RETRANSMIT, onInterestRetransmitted);
    
    if (onInterestRetransmitted != EMPTY_CALLBACK)
    {
      onInterestRetransmitted(*dynamic_cast<Consumer*>(m_context), interestWithDigest);
    }
    
    ConsumerInterestCallback onInterestToLeaveContext = EMPTY_CALLBACK;
    m_context->getContextOption(INTEREST_LEAVE_CNTX, onInterestToLeaveContext);
    if (onInterestToLeaveContext != EMPTY_CALLBACK)
    {
      onInterestToLeaveContext(*dynamic_cast<Consumer*>(m_context), interestWithDigest);
    }
    
    // because user could stop the context in one of the prev callbacks
    if (m_isRunning == false)
      return false;
    
    //retransmit
    m_interestsInFlight++;
    m_interestRetransmissions[segment]++;
    m_expressedInterests[segment] = m_face->expressInterest(interestWithDigest,
                                                bind(&ReliableDataRetrieval::onData, this, _1, _2),
                                                bind(&ReliableDataRetrieval::onTimeout, this, _1));
  }
  else
  {
    m_isRunning = false;
    return false;
  }

  return true;
}

void
ReliableDataRetrieval::onNackData(const ndn::Interest& interest, ndn::Data& data)
{
  if (m_isRunning == false)
    return;

  if (m_isFinalBlockNumberDiscovered)
  {
    if (data.getName().get(-3).toSegment() > m_finalBlockNumber)
    {
      return;
    } 
  }

  ConsumerDataVerificationCallback onDataToVerify = EMPTY_CALLBACK;
  m_context->getContextOption(DATA_TO_VERIFY, onDataToVerify);
  
  bool isDataSecure = false;
  
  if (onDataToVerify == EMPTY_CALLBACK)
  {
    // perform integrity check if possible
    if (data.getSignature().getType() == tlv::DigestSha256)
    {
      ndn::name::Component claimedDigest( data.getSignature().getValue().value(), 
                                          data.getSignature().getValue().value_size());
                   
      // recalculate digest
      m_keyChain.signWithSha256(data);
        
      ndn::name::Component actualDigest(data.getSignature().getValue().value(), 
                                        data.getSignature().getValue().value_size());
      
      if (claimedDigest.equals(actualDigest))
      {
        isDataSecure = true;
      }
      else 
      {
        isDataSecure = false;
      }
    }
    else
    {
      isDataSecure = true;
    }
  }
  else
  {
    if (onDataToVerify(*dynamic_cast<Consumer*>(m_context), data) == true) // runs verification routine
    {
      isDataSecure = true;
    }
  }
  
  if (isDataSecure)
  {
    checkFastRetransmissionConditions(interest);
  
    decreaseWindow();
    
    shared_ptr<ApplicationNack> nack = make_shared<ApplicationNack>(data);
    
    ConsumerNackCallback onNack = EMPTY_CALLBACK;
    m_context->getContextOption(NACK_ENTER_CNTX, onNack);
    if (onNack != EMPTY_CALLBACK)
    {
      onNack(*dynamic_cast<Consumer*>(m_context), *nack);
    }  
  
    switch (nack->getCode())
    {
      case ApplicationNack::DATA_NOT_AVAILABLE:
      {
        m_isRunning = false;
        break;
      }
    
      case ApplicationNack::NONE:
      {
        // TODO: reduce window size ?
        break;
      }
    
      case ApplicationNack::PRODUCER_DELAY: 
      {
        uint64_t segment = interest.getName().get(-1).toSegment();
        
        m_scheduledInterests[segment] = m_scheduler->scheduleEvent(time::milliseconds(nack->getDelay()),
                          bind(&ReliableDataRetrieval::retransmitFreshInterest, this, interest));
                          
        break;
      }
      
      default: break;
    }
  }
  else // if NACK is not verified
  {
    retransmitInterestWithExclude(interest, data);
  }
}

void
ReliableDataRetrieval::onContentData(const ndn::Interest& interest, ndn::Data& data)
{ 
  ConsumerDataVerificationCallback onDataToVerify = EMPTY_CALLBACK;
  m_context->getContextOption(DATA_TO_VERIFY, onDataToVerify);
  
  bool isDataSecure = false;

  if (onDataToVerify == EMPTY_CALLBACK)
  {
    // perform integrity check if possible
    if (data.getSignature().getType() == tlv::DigestSha256)
    {
      ndn::name::Component claimedDigest( data.getSignature().getValue().value(), 
                                          data.getSignature().getValue().value_size());
                   
      // recalculate digest
      m_keyChain.signWithSha256(data);
        
      ndn::name::Component actualDigest(data.getSignature().getValue().value(), 
                                        data.getSignature().getValue().value_size());
                                      
      if (claimedDigest.equals(actualDigest))
      {
        isDataSecure = true;
      }
      else
      {
        isDataSecure = false;
        retransmitInterestWithExclude(interest, data);
      }
    }
    else
    {
      isDataSecure = true;
    }
  }
  else
  {
    if (!data.getSignature().hasKeyLocator())
    {
      retransmitInterestWithExclude(interest, data);
      return;
    }

    // if data segment points to inlined manifest
    if (referencesManifest(data))
    {
      Name referencedManifestName = data.getSignature().getKeyLocator().getName();
      uint64_t manifestSegmentNumber = referencedManifestName.get(-1).toSegment();
    
      if (m_verifiedManifests.find(manifestSegmentNumber) == m_verifiedManifests.end())
      {
      // save segment for some time, because manifest can be out of order
      //std::cout << "SAVING SEGMENT for MANIFEST" << std::endl;
        m_unverifiedSegments.insert(
              std::pair<uint64_t, shared_ptr<Data> >( data.getName().get(-1).toSegment(), 
                                                      data.shared_from_this()));
      }
      else
      {
      //std::cout << "NEAREST M " << m_verifiedManifests[manifestSegmentNumber]->getName() << std::endl;
        isDataSecure = verifySegmentWithManifest(*(m_verifiedManifests[manifestSegmentNumber]), data);
      
        if (!isDataSecure)
        {
          //std::cout << "Retx Digest" << std::endl;
          retransmitInterestWithDigest( interest, data,  
                                      *m_verifiedManifests.find(manifestSegmentNumber)->second);
        }
      }
    }
    else // data segment points to the key
    {
      if (onDataToVerify(*dynamic_cast<Consumer*>(m_context), data) == true) // runs verification routine
      {
        isDataSecure = true;
      }
      else
      {
        retransmitInterestWithExclude(interest, data);
      }
    }
  }

  if (isDataSecure)
  {
    checkFastRetransmissionConditions(interest);

    if (!data.getFinalBlockId().empty())
    {
      m_isFinalBlockNumberDiscovered = true;
      m_finalBlockNumber = data.getFinalBlockId().toSegment();
    }

    // SegmentFetcher requires finalBlockId, this process is placed after getting final Block Id.
    increaseWindow();

    m_receiveBuffer[data.getName().get(-1).toSegment()] = data.shared_from_this();
    reassemble();  
  }
}

bool
ReliableDataRetrieval::referencesManifest(ndn::Data& data)
{
  Name keyLocatorPrefix = data.getSignature().getKeyLocator().getName().getPrefix(-1);
  Name dataPrefix = data.getName().getPrefix(-1);
  
  if (keyLocatorPrefix.equals(dataPrefix))
  {
    return true;
  }
  
  return false;
}

void
ReliableDataRetrieval::onTimeout(const ndn::Interest& interest)
{
  if (m_isRunning == false)
    return;

  m_interestsInFlight--;
  
  ConsumerInterestCallback onInterestExpired = EMPTY_CALLBACK;
  m_context->getContextOption(INTEREST_EXPIRED, onInterestExpired);
  if (onInterestExpired != EMPTY_CALLBACK)
  {
    onInterestExpired(*dynamic_cast<Consumer*>(m_context), const_cast<Interest&>(interest));
  }
  
  uint64_t segment = interest.getName().get(-1).toSegment();
  m_expressedInterests.erase(segment);
  m_scheduledInterests.erase(segment);
  
  if (m_isFinalBlockNumberDiscovered)
  {
    if (interest.getName().get(-1).toSegment() > m_finalBlockNumber)
      return;
  }
  
  decreaseWindow();

  int maxRetransmissions;
  m_context->getContextOption(INTEREST_RETX, maxRetransmissions);
  
  if(m_interestRetransmissions[segment] < maxRetransmissions)
  {
    Interest retxInterest(interest.getName());  // because we need new nonce
    int interestLifetime = DEFAULT_INTEREST_LIFETIME;
    m_context->getContextOption(INTEREST_LIFETIME, interestLifetime);
    retxInterest.setInterestLifetime(time::milliseconds(interestLifetime));
  
    SelectorHelper::applySelectors(retxInterest, m_context);
    
    // this is to inherit the exclusions from the timed out interest
    Exclude exclusion = retxInterest.getExclude();
    for(Exclude::const_iterator it = interest.getExclude().begin();
                                              it != interest.getExclude().end(); ++it)
    {
      exclusion.excludeRange(it->from, it->to);
    }
    retxInterest.setExclude(exclusion);
  
    ConsumerInterestCallback onInterestRetransmitted = EMPTY_CALLBACK;
    m_context->getContextOption(INTEREST_RETRANSMIT, onInterestRetransmitted);
    
    if (onInterestRetransmitted != EMPTY_CALLBACK)
    {
      onInterestRetransmitted(*dynamic_cast<Consumer*>(m_context), retxInterest);
    }
    
    ConsumerInterestCallback onInterestToLeaveContext = EMPTY_CALLBACK;
    m_context->getContextOption(INTEREST_LEAVE_CNTX, onInterestToLeaveContext);
    if (onInterestToLeaveContext != EMPTY_CALLBACK)
    {
      onInterestToLeaveContext(*dynamic_cast<Consumer*>(m_context), retxInterest);
    }
    
    // because user could stop the context in one of the prev callbacks
    if (m_isRunning == false)
      return;
    
    //retransmit
    m_interestsInFlight++;
    m_interestRetransmissions[segment]++;
    m_expressedInterests[segment] = m_face->expressInterest(retxInterest,
                                                bind(&ReliableDataRetrieval::onData, this, _1, _2),
                                                bind(&ReliableDataRetrieval::onTimeout, this, _1));
  }
  else
  {
    m_isRunning = false;
    reassemble(); // to pass up all content we have so far
  }
}

void
ReliableDataRetrieval::copyContent(Data& data)
{
  const Block content = data.getContent();
  m_contentBuffer.insert(m_contentBuffer.end(), &content.value()[0], &content.value()[content.value_size()]);
  
  if ((data.getName().get(-1).toSegment() == m_finalBlockNumber) || (!m_isRunning))
  {
    removeAllPendingInterests();
    removeAllScheduledInterests();
  
    // return content to the user
    ConsumerContentCallback onPayload = EMPTY_CALLBACK;
    m_context->getContextOption(CONTENT_RETRIEVED, onPayload);
    if (onPayload != EMPTY_CALLBACK)
    {
      onPayload(*dynamic_cast<Consumer*>(m_context), m_contentBuffer.data(), m_contentBuffer.size());
    }
    
    //reduce window size to prevent its speculative growth in case when consume() is called in loop
    int currentWindowSize = -1;
    m_context->getContextOption(CURRENT_WINDOW_SIZE, currentWindowSize);
    if (currentWindowSize > m_finalBlockNumber)
    {
      m_context->setContextOption(CURRENT_WINDOW_SIZE, (int)(m_finalBlockNumber));
    }
    
    m_isRunning = false;
  }
}

void
ReliableDataRetrieval::reassemble()
{  
  std::map<uint64_t, shared_ptr<Data> >::iterator head = m_receiveBuffer.find(m_lastReassembledSegment);
  while (head != m_receiveBuffer.end())
  {
    // do not copy from manifests
    if (head->second->getContentType() == CONTENT_DATA_TYPE)
    {
      copyContent(*(head->second));
    }
    
    m_receiveBuffer.erase(head);
    m_lastReassembledSegment++;
    head = m_receiveBuffer.find(m_lastReassembledSegment);
  }
}

bool
ReliableDataRetrieval::verifySegmentWithManifest(Manifest& manifestSegment, Data& dataSegment)
{
  //std::cout << "Verify Segment With MAnifest" << std::endl;
  bool result = false;
  
  for (std::list<Name>::const_iterator it = manifestSegment.catalogueBegin(); 
                                         it != manifestSegment.catalogueEnd(); ++it)
  {
    if (it->get(-2) == dataSegment.getName().get(-1)) // if segment numbers match
    {
      //re-calculate implicit digest
      const Block& block = dataSegment.wireEncode();
      ndn::ConstBufferPtr implicitDigest = ndn::crypto::sha256(block.wire(), block.size());
      
      // convert to name component for easier comparison
      name::Component digestComp = name::Component::fromImplicitSha256Digest(implicitDigest);
            
      if (digestComp.equals(it->get(-1)))
      {
        result = true;
        break;
      }
      else
      {
        break;
      }
    }
  }
  
  //if (!result)
  //  std::cout << "Segment failed verification by manifest" << result<< std::endl;

  return result;
}

name::Component
ReliableDataRetrieval::getDigestFromManifest(Manifest& manifestSegment, const Data& dataSegment)
{
  name::Component result;
  
  for (std::list<Name>::const_iterator it = manifestSegment.catalogueBegin(); 
                                         it != manifestSegment.catalogueEnd(); ++it)
  {
    if (it->get(-2) == dataSegment.getName().get(-1)) // if segment numbers match
    {
      result = it->get(-1);
      return result;
    }
  }
  
  return result;
}

void
ReliableDataRetrieval::checkFastRetransmissionConditions(const ndn::Interest& interest)
{
  uint64_t segNumber = interest.getName().get(-1).toSegment();
  m_receivedSegments[segNumber] = true;
  m_fastRetxSegments.erase(segNumber);
  
  uint64_t possiblyLostSegment = 0;
  uint64_t highestReceivedSegment = m_receivedSegments.rbegin()->first;

  for (uint64_t i = 0; i <= highestReceivedSegment; i++)
  {
    if (m_receivedSegments.find(i) == m_receivedSegments.end()) // segment is not received yet
    { 
      // segment has not been fast retransmitted yet
      if (m_fastRetxSegments.find(i) == m_fastRetxSegments.end()) 
      {
        possiblyLostSegment = i;
        uint8_t nOutOfOrderSegments = 0;
        for (uint64_t j = i; j <= highestReceivedSegment; j++)
        {
          if (m_receivedSegments.find(j) != m_receivedSegments.end())
          {
            nOutOfOrderSegments++;
            int fastRetxThreshold = DEFAULT_FAST_RETX_CONDITION;
            m_context->getContextOption(FAST_RETX_THRESHOLD, fastRetxThreshold);
            if (nOutOfOrderSegments == fastRetxThreshold)
            {
              if(m_isLogging){
                std::cout << ndn::time::toUnixTimestamp(time::system_clock::now()).count() << ", " << time::steady_clock::now() - getStartTime()
                << " OutOfOrder, Received segment:" << segNumber << ", OutOfOrderSegments:" << (int)nOutOfOrderSegments << ", PossiblyLostSegment:" << possiblyLostSegment << std::endl;
              }
              m_fastRetxSegments[possiblyLostSegment] = true;
              fastRetransmit(interest, possiblyLostSegment);
            }
          }
        }
      }
    }
  }
}

void
ReliableDataRetrieval::fastRetransmit(const ndn::Interest& interest, uint64_t segNumber)
{
  int maxRetransmissions;
  m_context->getContextOption(INTEREST_RETX, maxRetransmissions);
  
  if (m_interestRetransmissions[segNumber] < maxRetransmissions)
  {
    Name name = interest.getName().getPrefix(-1);
    name.appendSegment(segNumber);
      
    Interest retxInterest(name);
    SelectorHelper::applySelectors(retxInterest, m_context);
  
    // this is to inherit the exclusions from the lost interest
    Exclude exclusion = retxInterest.getExclude();
    for(Exclude::const_iterator it = interest.getExclude().begin();
                                it != interest.getExclude().end(); ++it)
    {
      exclusion.excludeRange(it->from, it->to);
    }
    retxInterest.setExclude(exclusion);
  
    ConsumerInterestCallback onInterestRetransmitted = EMPTY_CALLBACK;
    m_context->getContextOption(INTEREST_RETRANSMIT, onInterestRetransmitted);
    
    if (onInterestRetransmitted != EMPTY_CALLBACK)
    {
      onInterestRetransmitted(*dynamic_cast<Consumer*>(m_context), retxInterest);
    }
    
    ConsumerInterestCallback onInterestToLeaveContext = EMPTY_CALLBACK;
    m_context->getContextOption(INTEREST_LEAVE_CNTX, onInterestToLeaveContext);
    if (onInterestToLeaveContext != EMPTY_CALLBACK)
    {
      onInterestToLeaveContext(*dynamic_cast<Consumer*>(m_context), retxInterest);
    }
    
    // because user could stop the context in one of the prev callbacks
    if (m_isRunning == false)
      return;
    
    //retransmit
    m_interestsInFlight++;
    m_interestRetransmissions[segNumber]++;
    if(m_isLogging)
    {
      std::cout << ndn::time::toUnixTimestamp(time::system_clock::now()).count() << ", " << time::steady_clock::now() - getStartTime()
      << " RDR::fastReTx::inflight = " << m_interestsInFlight << ", windowSize = " << m_currentWindowSize << ", name = " << retxInterest.getName().toUri() << std::endl; 
    }
    m_expressedInterests[segNumber] = m_face->expressInterest(retxInterest,
                                          bind(&ReliableDataRetrieval::onData, this, _1, _2),
                                          bind(&ReliableDataRetrieval::onTimeout, this, _1));
  }
}

void
ReliableDataRetrieval::removeAllPendingInterests()
{
  bool isAsync = false;
  m_context->getContextOption(ASYNC_MODE, isAsync);
  
  if (!isAsync)
  {
    m_face->getIoService().stop(); // this worked.
    m_face->removeAllPendingInterests(); // faster, but destroys everything
  }
  else // slower, but destroys only necessary Interests
  {
    for(std::unordered_map<uint64_t, const PendingInterestId*>::iterator it = m_expressedInterests.begin();
                                                            it != m_expressedInterests.end(); ++it)
    {
      m_face->removePendingInterest(it->second);
    }
  }
  
  m_expressedInterests.clear();
}

void
ReliableDataRetrieval::removeAllScheduledInterests()
{
  for(std::unordered_map<uint64_t, EventId>::iterator it = m_scheduledInterests.begin();
                                                      it != m_scheduledInterests.end(); ++it)
  {
    m_scheduler->cancelEvent(it->second);
  }
  
  m_scheduledInterests.clear();
}

} //namespace ndn
